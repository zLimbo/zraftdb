package zkv

import (
	"fmt"
	"strings"
)

const usage = "gramma error, usage: get <key>, put <key> <val>, del <key>, all"

type ZKV struct {
	db map[string]string
}

func NewZKV() *ZKV {
	return &ZKV{db: map[string]string{}}
}

func (z *ZKV) Put(key, val string) {
	z.db[key] = val
}

func (z *ZKV) Get(key string) (string, bool) {
	val, ok := z.db[key]
	return val, ok
}

func (z *ZKV) Del(key string) {
	delete(z.db, key)
}

func (z *ZKV) All() string {
	result := ""
	for key, val := range z.db {
		result += fmt.Sprintf("%s:%s\n", key, val)
	}
	return result
}

func (z *ZKV) Execute(cmd string) string {
	words := strings.Split(cmd, " ")
	op := strings.ToLower(words[0])
	switch op {
	case "get":
		if len(words) != 2 {
			return usage
		}
		key := words[1]
		val, ok := z.Get(key)
		if !ok {
			return fmt.Sprintf("error: can't find this key '%s'", key)
		}
		return fmt.Sprintf("result: %s", val)
	case "put":
		if len(words) != 3 {
			return usage
		}
		key, val := words[1], words[2]
		z.Put(key, val)
		return "put ok"
	case "del":
		if len(words) != 2 {
			return usage
		}
		key := words[1]
		z.Del(key)
		return "del ok"
	case "all":
		if len(words) != 1 {
			return usage
		}
		return z.All()
	default:
		return fmt.Sprintf("syntax error, usage: %s\n", usage)
	}
}
