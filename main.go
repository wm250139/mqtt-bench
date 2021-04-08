package main

import (
	"context"
	"fmt"
	"github.com/eclipse/paho.golang/paho"
	"github.com/montanaflynn/stats"
	"github.com/olekukonko/tablewriter"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

func main() {
	bench := &Bench{
		Times: 1000,
		Targets: []Target{
			{
				// Measure connections over the loopback device
				Name:    "local",
				PubAddr: "localhost:1883",
				SubAddr: "localhost:1883",
			},
			{
				// Measure connections to a local VM
				// NB: This isn't measuring much, but was good to see the difference between loopback devices (above)
				//     and local but distinct devices.
				Name:    "local vm",
				PubAddr: "vm-ip:1883",
				SubAddr: "vm-ip:1883",
			},
			{
				// Measure connections to a remote (cloud-based) broker
				Name:    "remote vm",
				PubAddr: "remote.net:1883",
				SubAddr: "remote.net:1883",
			},
			{
				// Measure relying on HiveMQ's clustering to get messages from producer (local) to consumer (remote)
				// NB: Requires HiveMQ cluster set up beforehand
				Name:    "two-node cluster (local + remote)",
				PubAddr: "localhost:1884",
				SubAddr: "remote.net:1884",
			},
		},
	}

	start := time.Now()
	if err := bench.Run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Finished run in:", time.Since(start))
}

type Target struct {
	Name string

	PubAddr string
	SubAddr string

	pub *paho.Client
	sub *paho.Client
}

func (t *Target) init() error {
	if t.pub != nil && t.sub != nil {
		return nil
	}

	pub, err := newClient(fmt.Sprintf("%s-pub", t.Name), t.PubAddr)
	if err != nil {
		return err
	}
	t.pub = pub

	sub, err := newClient(fmt.Sprintf("%s-sub", t.Name), t.SubAddr)
	if err != nil {
		return err
	}
	t.sub = sub

	return nil
}

func (t *Target) Close() error {
	if t.pub != nil {
		if err := t.pub.Disconnect(&paho.Disconnect{}); err != nil {
			return err
		}
	}

	if t.sub != nil {
		if err := t.sub.Disconnect(&paho.Disconnect{}); err != nil {
			return err
		}
	}

	return nil
}

func newClient(name string, addr string) (*paho.Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	client := paho.NewClient()
	client.Conn = conn

	if _, err := client.Connect(context.Background(), &paho.Connect{
		ClientID:   name,
		KeepAlive:  5,
		CleanStart: true,
	}); err != nil {
		return nil, err
	}

	return client, nil
}

type Bench struct {
	Times   uint
	Targets []Target
}

func (b *Bench) Run() error {
	fmt.Printf("Number of messages: %d\n", b.Times)

	tw := tablewriter.NewWriter(os.Stdout)
	tw.SetAutoWrapText(false)
	tw.SetAutoFormatHeaders(false)
	tw.SetAutoMergeCellsByColumnIndex([]int{0})
	tw.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	tw.SetCenterSeparator("|")
	tw.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	tw.SetHeader([]string{"Configuration", "QoS", "Min", "Max", "Avg", "P95", "P99"})
	defer tw.Render()

	for _, t := range b.Targets {
		if err := t.init(); err != nil {
			return err
		}

		r0, err := runTarget(t, b.Times, 0)
		if err != nil {
			return err
		}
		tw.Append([]string{t.Name, "0", durStr(r0.Min), durStr(r0.Max), durStr(r0.Avg), durStr(r0.P95), durStr(r0.P99)})

		r1, err := runTarget(t, b.Times, 1)
		if err != nil {
			return err
		}
		tw.Append([]string{t.Name, "1", durStr(r1.Min), durStr(r1.Max), durStr(r1.Avg), durStr(r1.P95), durStr(r1.P99)})

		if err := t.Close(); err != nil {
			return err
		}
	}

	return nil
}

func runTarget(target Target, times uint, qos byte) (*Result, error) {
	var wg sync.WaitGroup
	wg.Add(int(times))
	var durations []time.Duration

	target.sub.Router = paho.NewSingleHandlerRouter(func(p *paho.Publish) {
		defer wg.Done()

		t, err := strconv.ParseInt(string(p.Payload), 10, 64)
		if err != nil {
			fmt.Println("Unable to parse int:", err)
			return
		}

		now := time.Now().UnixNano()
		dur := now - t
		durations = append(durations, time.Duration(dur))
	})

	if _, err := target.sub.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: map[string]paho.SubscribeOptions{
		"scox/bench": {QoS: qos},
	}}); err != nil {
		_ = target.Close()
		return nil, err
	}

	//defer target.sub.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{"scox/bench"}})

	for i := uint(0); i < times; i++ {
		now := time.Now().UnixNano()
		if _, err := target.pub.Publish(context.Background(), &paho.Publish{
			Topic:   "scox/bench",
			QoS:     qos,
			Payload: []byte(fmt.Sprint(now)),
		}); err != nil {
			_ = target.Close()
			return nil, err
		}
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	fmt.Printf("%s: (pub: %s, sub: %s, QoS: %d)\n", target.Name, target.PubAddr, target.SubAddr, qos)

	var min time.Duration = -1
	var max time.Duration = -1
	var sum time.Duration

	for _, dur := range durations {
		if min == -1 || dur < min {
			min = dur
		}
		if max == -1 || dur > max {
			max = dur
		}
		sum += dur
	}

	var avg time.Duration = sum / time.Duration(len(durations))

	fmt.Printf("  min: %s\n", durStr(min))
	fmt.Printf("  max: %s\n", durStr(max))
	fmt.Printf("  avg: %s\n", durStr(avg))

	// Calculate percentiles
	p95, err := percentile(durations, 95.0)
	if err != nil {
		return nil, err
	}
	p99, err := percentile(durations, 99.0)
	if err != nil {
		return nil, err
	}

	return &Result{Min: min, Max: max, Avg: avg, P95: p95, P99: p99}, nil
}

type Result struct {
	Min time.Duration
	Max time.Duration
	Avg time.Duration
	P95 time.Duration
	P99 time.Duration
}

func durStr(dur time.Duration) string {
	f := fmt.Sprint(dur)
	re := regexp.MustCompile(`(\d+)?\.?(\d+)(.+)`)
	matches := re.FindStringSubmatch(f)

	if len(matches) != 4 {
		fmt.Printf("unexpected number of matches: %s\n", matches)
		return f
	}

	maj := matches[1]
	min := matches[2]
	if len(min) > 2 {
		min = matches[2][0:2]
	}
	for len(min) < 2 {
		min = min + "0"
	}
	unit := matches[3]

	return fmt.Sprintf("%3s.%s %s", maj, min, unit)
}

func f2i(f float64) int64 {
	s := fmt.Sprintf("%.0f", f)
	i, err := strconv.Atoi(s)
	if err != nil {
		return -1
	}
	return int64(i)
}

func percentile(durations []time.Duration, p float64) (time.Duration, error) {
	var fd []float64
	for _, dur := range durations {
		fd = append(fd, float64(dur))
	}

	p, err := stats.Percentile(fd, p)
	if err != nil {
		return 0, err
	}

	return time.Duration(f2i(p)), nil
}
