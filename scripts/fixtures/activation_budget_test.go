package smp3core

import("net";"testing";"time";"sync/atomic")
type auditDropWrite struct{net.Conn}
func(c *auditDropWrite)Write(p []byte)(int,error){return len(p),nil}
func TestAuditProductionBudgetActivation(t *testing.T){
 activated:=make(chan struct{},1)
 cfg:=StreamConfig{SchedulerMode:StreamSchedulerAggregation,CapacityMode:StreamCapacityDynamic,BandwidthMbps:[]uint32{128,500},ChunkSize:65536,QueueFrames:256,MaxInflightFrames:1024,MaxReorderFrames:4096,AckInterval:20*time.Millisecond,RetransmitTimeout:1500*time.Millisecond,RecoveryTimeout:15*time.Second,ThresholdBytesPS:10_000_000,ActivationWindow:time.Second,OnActivate:func(){select{case activated<-struct{}{}:default:}}}
 c,app:=NewStreamEngine(cfg);defer c.Close();defer app.Close()
 a,b:=net.Pipe();defer b.Close()
 if err:=c.AttachLeg(0,&auditDropWrite{a},nil);err!=nil{t.Fatal(err)}
 time.Sleep(150*time.Millisecond)
 var accepted atomic.Uint64;done:=make(chan struct{})
 go func(){defer close(done);p:=make([]byte,65536);for{n,err:=app.Write(p);accepted.Add(uint64(n));if err!=nil{return}}}()
 defer func(){c.Close();app.Close();<-done}()
 select{case <-activated:t.Logf("activation=PASS accepted=%d",accepted.Load());case <-time.After(3*time.Second):t.Fatalf("activation=FAIL accepted=%d; primary drops all DATA, companion absent",accepted.Load())}
}
