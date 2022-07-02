package amqpjobs

import (
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func convHeaders(h amqp.Table) map[string][]string { //nolint:gocyclo
	ret := make(map[string][]string)
	for k := range h {
		switch t := h[k].(type) {
		case int:
			ret[k] = []string{strconv.Itoa(t)}
		case int8:
			ret[k] = []string{strconv.Itoa(int(t))}
		case int16:
			ret[k] = []string{strconv.Itoa(int(t))}
		case int32:
			ret[k] = []string{strconv.Itoa(int(t))}
		case int64:
			ret[k] = []string{strconv.Itoa(int(t))}
		case uint:
			ret[k] = []string{strconv.FormatUint(uint64(t), 10)}
		case uint8:
			ret[k] = []string{strconv.FormatUint(uint64(t), 10)}
		case uint16:
			ret[k] = []string{strconv.FormatUint(uint64(t), 10)}
		case uint32:
			ret[k] = []string{strconv.FormatUint(uint64(t), 10)}
		case uint64:
			ret[k] = []string{strconv.FormatUint(t, 10)}
		case float32:
			ret[k] = []string{strconv.FormatFloat(float64(t), 'f', 5, 64)}
		case float64:
			ret[k] = []string{strconv.FormatFloat(t, 'f', 5, 64)}
		case string:
			ret[k] = []string{t}
		case []string:
			ret[k] = t
		case bool:
			switch t {
			case true:
				ret[k] = []string{"true"}
			case false:
				ret[k] = []string{"false"}
			}
		case []byte:
			ret[k] = []string{string(t)}
		}
	}

	return ret
}
