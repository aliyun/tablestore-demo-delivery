package main

import (
	"fmt"
	"os"
	"strconv"
	"math/rand"
	"time"
	ots "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

var cities = []string{"Shanghai", "Chengdu", "Beijing", "Shenzhen"}

func newClient() ots.TableStoreApi {
	endpoint := os.Getenv("OTS_ENDPOINT")
	if len(endpoint) == 0 {
		panic(`require "OTS_ENDPOINT" in environ`)
	}
	instance := os.Getenv("OTS_INSTANCE")
	if len(instance) == 0 {
		panic(`require "OTS_INSTANCE" in environ`)
	}
	akId := os.Getenv("ACCESS_KEY_ID")
	if len(akId) == 0 {
		panic(`require "ACCESS_KEY_ID" in environ`)
	}
	akSecret := os.Getenv("ACCESS_KEY_SECRET")
	if len(akSecret) == 0 {
		panic(`require "ACCESS_KEY_SECRET" in environ`)
	}
	return ots.NewClient(endpoint, instance, akId, akSecret)
}

func getTps() int64 {
	tps := os.Getenv("TPS")
	if len(tps) == 0 {
		panic(`require "TPS" in environ`)
	}
	res, err := strconv.ParseInt(tps, 10, 64)
	if err != nil {
		panic(err)
	}
	return res
}

func genStandardUuid() string {
	uuid := make([]byte, 16)
	_, err := rand.Read(uuid)
	if err != nil {
		panic(err)
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
}

func genSourceCity() string {
	cnt := int64(len(cities))
	dice := rand.Int63() % (2 * cnt)
	if dice < cnt {
		return cities[dice]
	} else {
		idx := time.Now().Unix() % cnt
		return cities[idx]
	}
}

func genDestinationCity() string {
	cnt := int64(len(cities))
	dice := rand.Int63() % (2 * cnt)
	if dice < cnt {
		return cities[dice]
	} else {
		idx := time.Now().Unix() / cnt % cnt
		return cities[idx]
	}
}

func newDelivery(client ots.TableStoreApi) {
	deliveryId := genStandardUuid()
	
	fmt.Printf("%s 创建新快递单: %s\n", time.Now().Format(time.RFC3339Nano), deliveryId)
	{
		pkc := ots.PrimaryKeyColumn{
			ColumnName: "DeliveryId",
			Value: deliveryId}
		pk := ots.PrimaryKey{
			PrimaryKeys: []*ots.PrimaryKeyColumn{&pkc}}
		srcCity := ots.AttributeColumn{
			ColumnName: "SourceCity",
			Value: genSourceCity()}
		destCity := ots.AttributeColumn{
			ColumnName: "DestinationCity",
			Value: genDestinationCity()}
		chg := ots.PutRowChange{
			TableName: "PackageInfo",
			PrimaryKey: &pk,
			Columns: []ots.AttributeColumn{srcCity, destCity},
			Condition: &ots.RowCondition{
				RowExistenceExpectation: ots.RowExistenceExpectation_IGNORE}}
		req := ots.PutRowRequest{
			PutRowChange: &chg}
		_, err := client.PutRow(&req)
		if err != nil {
			panic(err)
		}
	}
	{
		pkDeliveryId := ots.PrimaryKeyColumn{
			ColumnName: "DeliveryId",
			Value: deliveryId}
		pkSeqNum := ots.PrimaryKeyColumn{
			ColumnName: "SeqNum",
			PrimaryKeyOption: ots.AUTO_INCREMENT}
		pk := ots.PrimaryKey{
			PrimaryKeys: []*ots.PrimaryKeyColumn{&pkDeliveryId, &pkSeqNum}}
		opType := ots.AttributeColumn{
			ColumnName: "OpType",
			Value: "NewDelivery"}
		scanner := ots.AttributeColumn{
			ColumnName: "Scanner",
			Value: genStandardUuid()}
		chg := ots.PutRowChange{
			TableName: "Logbook",
			PrimaryKey: &pk,
			Columns: []ots.AttributeColumn{opType, scanner},
			Condition: &ots.RowCondition{
				RowExistenceExpectation: ots.RowExistenceExpectation_IGNORE}}
		req := ots.PutRowRequest{
			PutRowChange: &chg}
		_, err := client.PutRow(&req)
		if err != nil {
			panic(err)
		}
	}

	for {
		sleepDuration := time.Duration((500 + rand.Int() % 500)) * time.Millisecond
		time.Sleep(sleepDuration)
		shallWeGoOn := rand.Int() % 5
		if shallWeGoOn == 0 {
			break
		}

		fmt.Printf("%s 转运中: %s\n", time.Now().Format(time.RFC3339Nano), deliveryId)
		pkDeliveryId := ots.PrimaryKeyColumn{
			ColumnName: "DeliveryId",
			Value: deliveryId}
		pkSeqNum := ots.PrimaryKeyColumn{
			ColumnName: "SeqNum",
			PrimaryKeyOption: ots.AUTO_INCREMENT}
		pk := ots.PrimaryKey{
			PrimaryKeys: []*ots.PrimaryKeyColumn{&pkDeliveryId, &pkSeqNum}}
		opType := ots.AttributeColumn{
			ColumnName: "OpType",
			Value: "SignIn"}
		scanner := ots.AttributeColumn{
			ColumnName: "Scanner",
			Value: genStandardUuid()}
		chg := ots.PutRowChange{
			TableName: "Logbook",
			PrimaryKey: &pk,
			Columns: []ots.AttributeColumn{opType, scanner},
			Condition: &ots.RowCondition{
				RowExistenceExpectation: ots.RowExistenceExpectation_IGNORE}}
		req := ots.PutRowRequest{
			PutRowChange: &chg}
		_, err := client.PutRow(&req)
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("%s 签收: %s\n", time.Now().Format(time.RFC3339Nano), deliveryId)
	{
		pkDeliveryId := ots.PrimaryKeyColumn{
			ColumnName: "DeliveryId",
			Value: deliveryId}
		pkSeqNum := ots.PrimaryKeyColumn{
			ColumnName: "SeqNum",
			PrimaryKeyOption: ots.AUTO_INCREMENT}
		pk := ots.PrimaryKey{
			PrimaryKeys: []*ots.PrimaryKeyColumn{&pkDeliveryId, &pkSeqNum}}
		opType := ots.AttributeColumn{
			ColumnName: "OpType",
			Value: "SignOff"}
		chg := ots.PutRowChange{
			TableName: "Logbook",
			PrimaryKey: &pk,
			Columns: []ots.AttributeColumn{opType},
			Condition: &ots.RowCondition{
				RowExistenceExpectation: ots.RowExistenceExpectation_IGNORE}}
		req := ots.PutRowRequest{
			PutRowChange: &chg}
		_, err := client.PutRow(&req)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	tps := getTps()
	rand.Seed(time.Now().UnixNano())
	client := newClient()

	tickDuration := time.Second / time.Duration(tps)
	tickChan := time.Tick(tickDuration)
	for _ = range tickChan {
		go newDelivery(client)
	}
}
