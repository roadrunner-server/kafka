package kafkajobs

import (
	"github.com/Shopify/sarama"
)

/*
SUPPORTED VERSIONS:

	V0_8_2_0  = newKafkaVersion(0, 8, 2, 0)
	V0_8_2_1  = newKafkaVersion(0, 8, 2, 1)
	V0_8_2_2  = newKafkaVersion(0, 8, 2, 2)
	V0_9_0_0  = newKafkaVersion(0, 9, 0, 0)
	V0_9_0_1  = newKafkaVersion(0, 9, 0, 1)
	V0_10_0_0 = newKafkaVersion(0, 10, 0, 0)
	V0_10_0_1 = newKafkaVersion(0, 10, 0, 1)
	V0_10_1_0 = newKafkaVersion(0, 10, 1, 0)
	V0_10_1_1 = newKafkaVersion(0, 10, 1, 1)
	V0_10_2_0 = newKafkaVersion(0, 10, 2, 0)
	V0_10_2_1 = newKafkaVersion(0, 10, 2, 1)
	V0_10_2_2 = newKafkaVersion(0, 10, 2, 2)
	V0_11_0_0 = newKafkaVersion(0, 11, 0, 0)
	V0_11_0_1 = newKafkaVersion(0, 11, 0, 1)
	V0_11_0_2 = newKafkaVersion(0, 11, 0, 2)
	V1_0_0_0  = newKafkaVersion(1, 0, 0, 0)
	V1_0_1_0  = newKafkaVersion(1, 0, 1, 0)
	V1_0_2_0  = newKafkaVersion(1, 0, 2, 0)
	V1_1_0_0  = newKafkaVersion(1, 1, 0, 0)
	V1_1_1_0  = newKafkaVersion(1, 1, 1, 0)
	V2_0_0_0  = newKafkaVersion(2, 0, 0, 0)
	V2_0_1_0  = newKafkaVersion(2, 0, 1, 0)
	V2_1_0_0  = newKafkaVersion(2, 1, 0, 0)
	V2_1_1_0  = newKafkaVersion(2, 1, 1, 0)
	V2_2_0_0  = newKafkaVersion(2, 2, 0, 0)
	V2_2_1_0  = newKafkaVersion(2, 2, 1, 0)
	V2_2_2_0  = newKafkaVersion(2, 2, 2, 0)
	V2_3_0_0  = newKafkaVersion(2, 3, 0, 0)
	V2_3_1_0  = newKafkaVersion(2, 3, 1, 0)
	V2_4_0_0  = newKafkaVersion(2, 4, 0, 0)
	V2_4_1_0  = newKafkaVersion(2, 4, 1, 0)
	V2_5_0_0  = newKafkaVersion(2, 5, 0, 0)
	V2_5_1_0  = newKafkaVersion(2, 5, 1, 0)
	V2_6_0_0  = newKafkaVersion(2, 6, 0, 0)
	V2_6_1_0  = newKafkaVersion(2, 6, 1, 0)
	V2_6_2_0  = newKafkaVersion(2, 6, 2, 0)
	V2_6_3_0  = newKafkaVersion(2, 6, 3, 0)
	V2_7_0_0  = newKafkaVersion(2, 7, 0, 0)
	V2_7_1_0  = newKafkaVersion(2, 7, 1, 0)
	V2_7_2_0  = newKafkaVersion(2, 7, 2, 0)
	V2_8_0_0  = newKafkaVersion(2, 8, 0, 0)
	V2_8_1_0  = newKafkaVersion(2, 8, 1, 0)
	V3_0_0_0  = newKafkaVersion(3, 0, 0, 0)
	V3_0_1_0  = newKafkaVersion(3, 0, 1, 0)
	V3_1_0_0  = newKafkaVersion(3, 1, 0, 0)
	V3_2_0_0  = newKafkaVersion(3, 2, 0, 0)
*/

func parseVersion(version string) sarama.KafkaVersion { //nolint:gocyclo
	switch version {
	case "0.8.2", "0.8.2.0":
		return sarama.V0_8_2_0
	case "0.8.2.1":
		return sarama.V0_8_2_1
	case "0.8.2.2":
		return sarama.V0_8_2_2
	case "0.9.0", "0.9.0.0":
		return sarama.V0_9_0_0
	case "0.10.0", "0.10.0.0":
		return sarama.V0_10_0_0
	case "0.10.0.1":
		return sarama.V0_10_0_1
	case "0.10.1.1":
		return sarama.V0_10_1_1
	case "0.10.2.0":
		return sarama.V0_10_2_0
	case "0.10.2.1":
		return sarama.V0_10_2_1
	case "0.10.2.2":
		return sarama.V0_10_2_2
	case "0.11.0.0":
		return sarama.V0_11_0_0
	case "0.11.0.1":
		return sarama.V0_11_0_1
	case "0.11.0.2":
		return sarama.V0_11_0_2
	case "1.0.0", "1.0", "1.0.0.0":
		return sarama.V1_0_0_0
	case "1.0.1", "1.0.1.0":
		return sarama.V1_0_1_0
	case "1.0.2", "1.0.2.0":
		return sarama.V1_0_2_0
	case "1.1.0", "1.1.0.0":
		return sarama.V1_1_0_0
	case "1.1.1", "1.1.1.0":
		return sarama.V1_1_1_0
	case "2.0", "2.0.0", "2.0.0.0":
		return sarama.V2_0_0_0
	case "2.0.1", "2.0.1.0":
		return sarama.V2_0_1_0
	case "2.1", "2.1.0", "2.1.0.0":
		return sarama.V2_1_0_0
	case "2.1.1", "2.1.1.0":
		return sarama.V2_1_1_0
	case "2.2.0", "2.2", "2.2.0.0":
		return sarama.V2_2_0_0
	case "2.2.1", "2.2.1.0":
		return sarama.V2_2_1_0
	case "2.2.2", "2.2.2.0":
		return sarama.V2_2_2_0
	case "2.3", "2.3.0", "2.3.0.0":
		return sarama.V2_3_0_0
	case "2.3.1", "2.3.1.0":
		return sarama.V2_3_1_0
	case "2.4.0", "2.4.0.0":
		return sarama.V2_4_0_0
	case "2.4.1", "2.4.1.0":
		return sarama.V2_4_1_0
	case "2.5", "2.5.0", "2.5.0.0":
		return sarama.V2_5_0_0
	case "2.5.1", "2.5.1.0":
		return sarama.V2_5_1_0
	case "2.6", "2.6.0", "2.6.0.0":
		return sarama.V2_6_0_0
	case "2.6.1", "2.6.1.0":
		return sarama.V2_6_1_0
	case "2.6.2", "2.6.2.0":
		return sarama.V2_6_2_0
	case "2.6.3", "2.6.3.0":
		return sarama.V2_6_3_0
	case "2.7", "2.7.0", "2.7.0.0":
		return sarama.V2_7_0_0
	case "2.7.1", "2.7.1.0":
		return sarama.V2_7_1_0
	case "2.7.2", "2.7.2.0":
		return sarama.V2_7_2_0
	case "2.8", "2.8.0", "2.8.0.0":
		return sarama.V2_8_0_0
	case "2.8.1", "2.8.1.0":
		return sarama.V2_8_1_0
	case "3.0", "3.0.0", "3.0.0.0":
		return sarama.V3_0_0_0
	case "3.0.1", "3.0.1.0":
		return sarama.V3_0_1_0
	case "3.1", "3.1.0", "3.1.0.0":
		return sarama.V3_1_0_0
	case "3.2", "3.2.0", "3.2.0.0":
		return sarama.V3_2_0_0
	default:
		return sarama.DefaultVersion
	}
}
