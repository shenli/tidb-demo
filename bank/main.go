package main

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
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
	friends map[string](chan int)
	recvCh  chan int
	working bool
}

func (c *Customer) randomMoney(int upper) int {
	// Get random money greater than or equals 0, smaller than or equals than upper
	return 0
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

// Random transfer
func (c *Customer) randomTransfer() error {
	n := c.randomMoney(c.balance)
	f, ch := c.randomFriends()
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

}

func (c *Customer) run() error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case n := <-c.recvCh:
			c.balance += n
		case <-ticker.C:
		}
	}
}
func main() {
}
