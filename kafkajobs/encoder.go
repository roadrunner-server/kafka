package kafkajobs

type JobKVEncoder struct {
	value []byte
}

func (j JobKVEncoder) Encode() ([]byte, error) {
	return j.value, nil
}

func (j JobKVEncoder) Length() int {
	return len(j.value)
}
