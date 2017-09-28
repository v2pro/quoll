package event

import (
	"os"
)

func Add(eventType string, eventJson []byte) error {
	file, err := os.OpenFile(
		"/tmp/" + eventType + ".log.2017092708", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	start := 0
	for i, b := range eventJson {
		if b == '\n' {
			_, err = file.Write(eventJson[start:i])
			if err != nil {
				return err
			}
			_, err = file.Write([]byte{'\\', 'n'})
			if err != nil {
				return err
			}
			start = i + 1
		}
	}
	_, err = file.Write(eventJson[start:])
	if err != nil {
		return err
	}
	_, err = file.Write([]byte{'\n'})
	if err != nil {
		return err
	}
	return nil
}
