package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var (
	dsn                  = flag.String("dsn", "root:@(127.0.0.1:4000)/test", "DB dsn to use.")
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
	if err != nil {
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
	if err != nil {
		return errors.Trace(err)
	}
	err = tx.Commit()
	return errors.Trace(err)
}

// Get balance for customer from bank
func (b *Bank) getBalance(id string) (int, error) {
	tx, err := b.db.Begin()
	if err != nil {
		return -1, errors.Trace(err)
	}
	r := tx.QueryRow(fmt.Sprintf("select balance from customers where id=%s", id))
	var bb int
	err = r.Scan(&bb)
	if err != nil {
		return -1, errors.Trace(err)
	}
	return bb, nil
}

// Transfer num money.
func (b *Bank) transfer(from, to string, num int) error {
	tx, err := b.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(fmt.Sprintf("update customers set balance=balance-%d where id=%s", num, from))
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(fmt.Sprintf("update customers set balance=balance+%d where id=%s", num, to))
	if err != nil {
		return errors.Trace(err)
	}
	err = tx.Commit()
	return errors.Trace(err)
}

func (b *Bank) CreateAccount(id string, balance int) error {
	tx, err := b.db.Begin()
	st := fmt.Sprintf("insert into test.customers values (%s, %d)", id, balance)
	_, err = tx.Exec(st)
	if err != nil {
		return errors.Trace(err)
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("Find error when commit: %v", err)
		return errors.Trace(err)
	}
	return nil
}

func (b *Bank) Open(dsn string) error {
	var err error
	b.db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Errorf("Open %s error: %v", dsn, err)
		return errors.Trace(err)
	}

	tx, err := b.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec("drop table if exists customers;")
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec("create table customers (id VARCHAR(32) PRIMARY KEY, balance INT);")
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
	if n == 0 {
		return nil
	}
	log.Infof("[Customer_%s] Begin deposite %d into bank.", c.id, n)
	err := c.bank.deposit(c.id, n)
	if err != nil {
		//succ
		return errors.Trace(err)
	}
	c.wallet -= n
	c.balance += n
	log.Infof("[Customer_%s] Deposite %d into bank success.", c.id, n)
	return nil
}

// Random withdraw
func (c *Customer) randomWithdraw() error {
	n := c.randomMoney(c.balance)
	if n == 0 {
		return nil
	}
	log.Infof("[Customer_%s] Begin withdraw %d money from bank.", c.id, n)
	err := c.bank.withdraw(c.id, n)
	if err != nil {
		//succ
		return errors.Trace(err)
	}
	c.wallet += n
	c.balance -= n
	log.Infof("[Customer_%s] Withdraw %d money from bank success.", c.id, n)
	return nil
}

func (c *Customer) randomFriend() (string, chan int) {
	i := int(rand.Int31n(int32(len(c.friends))))
	return c.friends[i].id, c.friends[i].recvCh
}

// Random transfer
func (c *Customer) randomTransfer() error {
	n := c.randomMoney(c.balance)
	if n == 0 {
		return nil
	}
	f, ch := c.randomFriend()
	log.Infof("[Customer_%s] Begin transfer %d money to Customer_%s.", c.id, n, f)
	err := c.bank.transfer(c.id, f, n)
	if err != nil {
		return errors.Trace(err)
	}
	ch <- n
	c.balance -= n
	log.Infof("[Customer_%s] Transfer %d money to Customer_%s succ.", c.id, n, f)
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
	action := int(rand.Int31n(3))
	if action == 0 {
		return c.randomDeposit()
	} else if action == 1 {
		return c.randomWithdraw()
	} else if action == 2 {
		return c.randomTransfer()
	}
	return errors.Errorf("Wrong Type of action ", action)
}

func (c *Customer) run(wg sync.WaitGroup) error {
	ticker := time.NewTicker(1 * time.Second)
	cnt := 0
	defer func() {
		ticker.Stop()
		wg.Done()
		log.Infof("Customer_%s QUIT succ", c.id)
	}()
	for {
		select {
		case n := <-c.recvCh:
			c.balance += n
		case <-ticker.C:
			cnt += 1
			if cnt > 60 {
				return nil
			}
			log.Infof("[Customer_%s] round %d", c.id, cnt)
			err := c.randomDo()
			if err != nil {
				log.Errorf("ERRORRRRR!!!!")
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
	initWallet := 5000
	for i := 0; i < *persons; i++ {
		id := fmt.Sprintf("%d", i)
		c := &Customer{
			id:      id,
			wallet:  initWallet,
			balance: 0,
			friends: make([]*Customer, 0, *persons-1),
			recvCh:  make(chan int),
			bank:    bank,
		}
		err := bank.CreateAccount(c.id, 0)
		if err != nil {
			log.Errorf("Create user failed: %v", err)
			return
		}
		customers = append(customers, c)
		log.Infof("Create user %s succ", c.id)
	}
	for i := 0; i < *persons-1; i++ {
		for j := i + 1; j < *persons; j++ {
			c1 := customers[i]
			c2 := customers[j]
			c1.friends = append(c1.friends, c2)
			c2.friends = append(c2.friends, c1)
		}
	}
	var wg sync.WaitGroup
	wg.Add(len(customers))
	fmt.Println("Bank Demo begin.......")
	for _, c := range customers {
		go c.run(wg)
	}
	wg.Wait()
	fmt.Println("Bank Demo End!")
	fmt.Println("Start checking........")
	money := 0
	for _, c := range customers {
		bb, err1 := bank.getBalance(c.id)
		if err1 != nil {
			log.Errorf("Get balance error: %v", err1)
			return
		}
		if bb != c.balance {
			log.Errorf("Balance unmatch for %s, %d:%d", c.id, c.balance, bb)
			return
		}
		money += c.wallet + bb
	}
	total := *persons * initWallet
	if money != total {
		log.Errorf("Total money unmatch: %d : %d", money, total)
		return
	}
	fmt.Println("Success!!!!")
}
