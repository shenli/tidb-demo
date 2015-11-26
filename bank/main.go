package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var (
	dsn                  = flag.String("dsn", "", "DB dsn to use.")
	persons              = flag.Int("persons", 5, "Number of concurrent persions.")
	balanceCheckInterval = flag.Duration("balance-check-interval", 1*time.Second, "Interval of balance check.")
)

type Bank struct {
	db *sql.DB
}

// People with id deposit num money into bank
func (b *Bank) deposit(id string, num int) error {
	tx, err := b.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(fmt.Sprintf("update customers set balance=balance+%d where id=%s", num, id))
	if err == nil {
		return errors.Trace(err)
	}
	err = tx.Commit()
	return errors.Trace(err)
}

// People with id withdraw num money from bank
func (b *Bank) withdraw(id string, num int) error {
	tx, err := b.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(fmt.Sprintf("update customers set balance=balance-%d where id=%s", num, id))
	if err == nil {
		return errors.Trace(err)
	}
	err = tx.Commit()
	return errors.Trace(err)
}

// Transfer num money.
func (b *Bank) transfer(from, to string, num int) error {
	tx, err := b.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(fmt.Sprintf("update customers set balance=balance-%d where id=%s", num, from))
	if err == nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(fmt.Sprintf("update customers set balance=balance+%d where id=%s", num, to))
	if err == nil {
		return errors.Trace(err)
	}
	err = tx.Commit()
	return errors.Trace(err)
}

func (b *Bank) Open(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.Trace(err)
	}
	b.db = db

	tx, err := b.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec("drop table if exists customers;")
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec("create table customers (id TEXT PRIMARY KEY, balance INT);")
	if err != nil {
		return errors.Trace(err)
	}
	err = tx.Commit()
	return errors.Trace(err)
}

func (b *Bank) Close(dsn string) error {
	if b.db == nil {
		return nil
	}
	return b.db.Close()
}

type Customer struct {
	id      string
	wallet  int   // Money in the wallet
	balance int   // Balance in the bank
	bank    *Bank // Bank
	friends []*Customer
	recvCh  chan int
	working bool
}

func (c *Customer) randomMoney(upper int) int {
	// Get random money greater than or equals 0, smaller than or equals than upper
	if upper == 0 {
		return 0
	}
	return int(rand.Int31n(int32(upper)))
}

// Random deposite
func (c *Customer) randomDeposit() error {
	n := c.randomMoney(c.wallet)
	err := c.bank.deposit(c.id, n)
	if err != nil {
		//succ
		return errors.Trace(err)
	}
	c.wallet -= n
	c.balance += n
	log.Infof("[Customer_%s] Deposite %d from bank.", c.id, n)
	return nil
}

// Random withdraw
func (c *Customer) randomWithdraw() error {
	n := c.randomMoney(c.balance)
	err := c.bank.withdraw(c.id, n)
	if err != nil {
		//succ
		return errors.Trace(err)
	}
	c.wallet += n
	c.balance -= n
	return nil
}

func (c *Customer) randomFriend() (string, chan int) {
	i := int(rand.Int31n(int32(len(c.friends))))
	return c.friends[i].id, c.friends[i].recvCh
}

// Random transfer
func (c *Customer) randomTransfer() error {
	n := c.randomMoney(c.balance)
	f, ch := c.randomFriend()
	err := c.bank.transfer(c.id, f, n)
	if err != nil {
		return errors.Trace(err)
	}
	ch <- n
	c.balance -= n
	return nil
}

func (c *Customer) randomDo() error {
	if c.working {
		return nil
	}
	c.working = true
	defer func() {
		c.working = false
	}()

	// random action
	// GetRandom Action
	action := rand.Int31n(3)
	switch action {
	case 0:
		return c.randomDeposit()
	case 1:
		return c.randomWithdraw()
	case 2:
		return c.randomTransfer()
	}
	return nil
}

func (c *Customer) run() error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case n := <-c.recvCh:
			c.balance += n
		case <-ticker.C:
			err := c.randomDo()
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func main() {
	bank := &Bank{}
	err := bank.Open(*dsn)
	if err != nil {
		fmt.Println("Open Bank error: ", err)
	}
	customers := make([]*Customer, 0, *persons)
	for i := 0; i < *persons; i++ {
		c := &Customer{
			id:      string(i),
			wallet:  5000,
			balance: 0,
			friends: make([]*Customer, 0, *persons-1),
			recvCh:  make(chan int),
		}
		customers = append(customers, c)
	}
	for i := 0; i < *persons-1; i++ {
		for j := i + 1; j < *persons; j++ {
			c1 := customers[i]
			c2 := customers[j]
			c1.friends = append(c1.friends, c2)
			c2.friends = append(c2.friends, c1)
		}
	}
	for _, c := range customers {
		go c.run()
	}
}
