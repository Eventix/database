package database

import (
	"fmt"
	"sync"

	"log"
	"strings"

    "database/sql"
)

type Keyer interface {
	Key() string
	Company() string
}

type Scannable interface {
	Scan(...interface{}) error
}

type LoaderFunc func(s Scannable) (Keyer, error)

type Base struct {
	single    LoaderFunc
	table     string
	columns   []string
	keyName   string
	immutable bool
	editable  bool
	lm        sync.Map
	db 	      *sql.DB

	cns string
}

func New(db *sql.DB, table string, columns []string, keyname string, single LoaderFunc, isImmutable, canBeEdited bool) *Base {
	return &Base{
		single,
		table,
		columns,
		keyname,
		isImmutable,
		canBeEdited,
		sync.Map{},
		"`" + strings.Join(columns, "`, `") + "`",
		db,
	}
}

func (b *Base) FromKey(id interface{}) (Keyer, error) {
	if v, ok := b.lm.Load(id); ok {
		return v.(Keyer), nil
	}

	v, err := b.single(b.db.QueryRow(fmt.Sprintf("SELECT %v FROM %v WHERE %v = ?", b.cns, b.table, b.keyName), id))

	if err == nil {
		b.lm.Store(id, v)
	}

	return v, err
}

func (b *Base) FromKeys(ids ...interface{}) (map[string]Keyer, error) {
	firstIt := true
	var oe error

	m := make(map[string]Keyer)

	sqlIds := make([]interface{}, len(ids))

doWork:
	a := 0
	for _, id := range ids {
		if v, ok := b.lm.Load(id); ok {
			kb := v.(Keyer)
			m[kb.Key()] = kb
		} else {
			sqlIds[a] = id
			a++
		}
	}

	// When we to add stuff, reslice and retrieve from database
	if a > 0 && firstIt {

		firstIt = false
		sqlIds = sqlIds[:a]

		q := fmt.Sprintf("SELECT %v FROM %v WHERE %v in (?%v)", b.cns, b.table, b.keyName, strings.Repeat(", ?", a-1))
		oe = b.FromQuery(q, sqlIds...)

		// Load the new keys from the local versions
		ids = sqlIds
		goto doWork
	}

	return m, oe
}

func (b *Base) LoadAll() error {
	return b.FromQuery(fmt.Sprintf("SELECT %v FROM %v", b.cns, b.table))
}

func (b *Base) FromQuery(q string, v ...interface{}) error {
	rows, err := b.db.Query(q, v...)

	if err != nil {
		log.Println("Error retrieving multiple kickbacks", err)
		return err
	}
	defer rows.Close()
	var oe error

	for rows.Next() {
		k, e := b.single(rows)
		if e != nil && oe == nil {
			log.Println("Error retrieving single from database")
			oe = e
		}

		// Store in syncmap
		b.lm.Store(k.Key(), k)
	}

	return oe
}

func (b *Base) Iterate(a func(interface{}, interface{}) bool) {
	b.lm.Range(a)
}

func (b *Base) PIterate(a func(interface{}, interface{}) bool) {
	b.lm.Range(func(k, v interface{}) bool {
		go a(k, v)
		return true
	})
}

func (b *Base) Delete(key interface{}) {
	b.lm.Delete(key)
}
