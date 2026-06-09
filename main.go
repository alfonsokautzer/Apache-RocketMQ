package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
)

// ConsumerOffsetManager manages the in-memory offset table and persists it.
type ConsumerOffsetManager struct {
	mu       sync.RWMutex
	offsets  map[string]int64 // key: topic@group
	filePath string
}

func NewConsumerOffsetManager(filePath string) *ConsumerOffsetManager {
	return &ConsumerOffsetManager{
		offsets:  make(map[string]int64),
		filePath: filePath,
	}
}

func (m *ConsumerOffsetManager) Commit(topic, group string, offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := topic + "@" + group
	if current, exists := m.offsets[key]; !exists || offset > current {
		m.offsets[key] = offset
	}
}

func (m *ConsumerOffsetManager) Query(topic, group string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := topic + "@" + group
	return m.offsets[key]
}

func (m *ConsumerOffsetManager) Persist() error {
	m.mu.RLock()
	data, err := json.Marshal(m.offsets)
	m.mu.RUnlock()
	if err != nil {
		return err
	}
	return ioutil.WriteFile(m.filePath, data, 0644)
}

func (m *ConsumerOffsetManager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := ioutil.ReadFile(m.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &m.offsets)
}

// Broker represents a RocketMQ broker instance.
type Broker struct {
	id              string
	role            string // "master" or "slave"
	offsetManager   *ConsumerOffsetManager
	isTransitioning int32 // atomic boolean
	mu              sync.RWMutex
}

func NewBroker(id string, role string, filePath string) *Broker {
	return &Broker{
		id:            id,
		role:          role,
		offsetManager: NewConsumerOffsetManager(filePath),
	}
}

func (b *Broker) CommitOffset(topic, group string, offset int64) error {
	if atomic.LoadInt32(&b.isTransitioning) == 1 {
		return fmt.Errorf("broker %s is transitioning, commit rejected", b.id)
	}

	b.mu.RLock()
	role := b.role
	b.mu.RUnlock()

	if role != "master" {
		return fmt.Errorf("broker %s is not master, cannot commit offset", b.id)
	}

	b.offsetManager.Commit(topic, group, offset)
	return nil
}

func (b *Broker) QueryOffset(topic, group string) (int64, error) {
	if atomic.LoadInt32(&b.isTransitioning) == 1 {
		return 0, fmt.Errorf("broker %s is transitioning, query rejected", b.id)
	}

	b.mu.RLock()
	role := b.role
	b.mu.RUnlock()

	if role != "master" {
		return 0, fmt.Errorf("broker %s is not master, cannot query offset", b.id)
	}

	return b.offsetManager.Query(topic, group), nil
}

func (b *Broker) ChangeToSlave(newMaster *Broker) error {
	fmt.Printf("[Broker %s] Transitioning to SLAVE...\n", b.id)
	atomic.StoreInt32(&b.isTransitioning, 1)
	defer atomic.StoreInt32(&b.isTransitioning, 0)

	fmt.Printf("[Broker %s] Force flushing offsets to disk...\n", b.id)
	if err := b.offsetManager.Persist(); err != nil {
		return fmt.Errorf("failed to persist offsets: %v", err)
	}

	b.mu.Lock()
	b.role = "slave"
	b.mu.Unlock()

	if newMaster != nil {
		fmt.Printf("[Broker %s] Syncing offsets from new master %s...\n", b.id, newMaster.id)
		newMaster.offsetManager.mu.RLock()
		defer newMaster.offsetManager.mu.RUnlock()
		
		b.offsetManager.mu.Lock()
		for k, v := range newMaster.offsetManager.offsets {
			b.offsetManager.offsets[k] = v
		}
		b.offsetManager.mu.Unlock()
		_ = b.offsetManager.Persist()
	}

	fmt.Printf("[Broker %s] Successfully transitioned to SLAVE.\n", b.id)
	return nil
}

func (b *Broker) ChangeToMaster() error {
	fmt.Printf("[Broker %s] Transitioning to MASTER...\n", b.id)
	atomic.StoreInt32(&b.isTransitioning, 1)
	defer atomic.StoreInt32(&b.isTransitioning, 0)

	fmt.Printf("[Broker %s] Loading offsets from disk...\n", b.id)
	if err := b.offsetManager.Load(); err != nil {
		return fmt.Errorf("failed to load offsets: %v", err)
	}

	b.mu.Lock()
	b.role = "master"
	b.mu.Unlock()

	fmt.Printf("[Broker %s] Successfully transitioned to MASTER.\n", b.id)
	return nil
}

func main() {
	fmt.Println("Starting RocketMQ Broker Failover Simulation...")

	brokerAFile := "consumerOffset_A.json"
	brokerBFile := "consumerOffset_B.json"

	defer os.Remove(brokerAFile)
	defer os.Remove(brokerBFile)

	brokerA := NewBroker("Broker-A", "master", brokerAFile)
	brokerB := NewBroker("Broker-B", "slave", brokerBFile)

	topic := "TestTopic"
	group := "TestGroup"

	fmt.Println("\n--- Step 1: Committing offsets to Master (Broker-A) ---")
	for i := int64(10); i <= 50; i += 10 {
		if err := brokerA.CommitOffset(topic, group, i); err != nil {
			fmt.Printf("Commit failed: %v\n", err)
		} else {
			fmt.Printf("Committed offset: %d\n", i)
		}
	}

	offset, err := brokerA.QueryOffset(topic, group)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
	} else {
		fmt.Printf("Queried offset from Broker-A: %d\n", offset)
	}

	fmt.Println("\n--- Step 2: Simulating Failover (Broker-A steps down, Broker-B becomes Master) ---")
	
	if err := brokerA.ChangeToSlave(nil); err != nil {
		fmt.Printf("Broker-A step down failed: %v\n", err)
	}

	data, err := ioutil.ReadFile(brokerAFile)
	if err == nil {
		_ = ioutil.WriteFile(brokerBFile, data, 0644)
	}

	if err := brokerB.ChangeToMaster(); err != nil {
		fmt.Printf("Broker-B transition to Master failed: %v\n", err)
	}

	fmt.Println("\n--- Step 3: Querying offsets from the new Master (Broker-B) ---")
	newOffset, err := brokerB.QueryOffset(topic, group)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
	} else {
		fmt.Printf("Queried offset from Broker-B: %d\n", newOffset)
		if newOffset == 50 {
			fmt.Println("SUCCESS: Zero Offset Regression verified!")
		} else {
			fmt.Printf("FAILURE: Offset rolled back to %d\n", newOffset)
		}
	}
}