package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"sync"
	"time"
)

func main() {
	//benchmarkNonPoolApproach()
	benchmarkPoolApproach()
}

type conn struct {
	db *sql.DB
}

func benchmarkNonPoolApproach() {
	startTime := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			con := newConn()
			execute(con)
			con.db.Close()
		}()
	}
	wg.Wait()
	fmt.Println("Non connection pool approach benchmarking: ", time.Since(startTime))
}

type ConnPool struct {
	pool chan *conn
}

func (c *ConnPool) enqueueConn(con *conn) {
	c.pool <- con
}

func (c *ConnPool) getConn() *conn {
	return <-c.pool
}

func (c *ConnPool) closePool() {
	close(c.pool)
	for conn := range c.pool {
		conn.db.Close()
	}
}

func NewPool(poolSize int) *ConnPool {
	fmt.Printf("Creating connection pool with %d connections\n", poolSize)
	pool := make(chan *conn, poolSize)
	for i := 0; i < poolSize; i++ {
		pool <- newConn()
	}

	return &ConnPool{
		pool: pool,
	}
}

func benchmarkPoolApproach() {
	startTime := time.Now()
	var wg sync.WaitGroup
	conPool := NewPool(10)
	fmt.Println("Spawning goroutines...")
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := conPool.getConn()
			execute(db)
			conPool.enqueueConn(db)
		}()
	}
	fmt.Println("Waiting for all goroutines to finish...")
	wg.Wait()
	conPool.closePool()
	fmt.Println("Connection pool approach benchmarking: ", time.Since(startTime))
}

func newConn() *conn {
	db, err := sql.Open("mysql", "user:password@tcp(localhost:3307)/db")
	if err != nil {
		log.Fatal(err)
	}
	return &conn{
		db,
	}
}

func execute(con *conn) {
	_, err := con.db.Query("SELECT sleep(0.01);")
	if err != nil {
		panic(err)
	}
}
