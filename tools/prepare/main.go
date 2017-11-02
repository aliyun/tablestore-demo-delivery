package main

import (
	"fmt"
	"os"
	ots "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

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

func createJournalTable(client ots.TableStoreApi) {
	fmt.Println("创建快递流水表")
	id := "DeliveryId"
	typeStr := ots.PrimaryKeyType_STRING
	seqNum := "SeqNum"
	typeInt := ots.PrimaryKeyType_INTEGER
	optInc := ots.AUTO_INCREMENT
	schema := []*ots.PrimaryKeySchema{
		&ots.PrimaryKeySchema{
			Name: &id,
			Type: &typeStr},
		&ots.PrimaryKeySchema{
			Name: &seqNum,
			Type: &typeInt,
			Option: &optInc}}
	meta := ots.TableMeta{
		TableName: "Logbook",
		SchemaEntry: schema}
	req := ots.CreateTableRequest{
		TableMeta: &meta,
		TableOption: &ots.TableOption{
			TimeToAlive: -1,
			MaxVersion: 1},
		ReservedThroughput: &ots.ReservedThroughput{
			Readcap: 0,
			Writecap: 0},
		StreamSpec: &ots.StreamSpecification{
			EnableStream: true,
			ExpirationTime: 24}}
	_, err := client.CreateTable(&req)
	if err != nil {
		panic(err)
	}
	fmt.Println("创建快递流水表，成功")
}

func createDeliveryInfoTable(client ots.TableStoreApi) {
	fmt.Println("创建快递单信息表")
	id := "DeliveryId"
	typeStr := ots.PrimaryKeyType_STRING
	schema := []*ots.PrimaryKeySchema{
		&ots.PrimaryKeySchema{
			Name: &id,
			Type: &typeStr}}
	meta := ots.TableMeta{
		TableName: "PackageInfo",
		SchemaEntry: schema}
	req := ots.CreateTableRequest{
		TableMeta: &meta,
		TableOption: &ots.TableOption{
			TimeToAlive: -1,
			MaxVersion: 1},
		ReservedThroughput: &ots.ReservedThroughput{
			Readcap: 0,
			Writecap: 0}}
	_, err := client.CreateTable(&req)
	if err != nil {
		panic(err)
	}
	fmt.Println("创建快递单信息表，成功")
}

func createFlywireTable(client ots.TableStoreApi) {
	fmt.Println("创建飞线表")
	id := "Timestamp"
	typeInt := ots.PrimaryKeyType_INTEGER
	schema := []*ots.PrimaryKeySchema{
		&ots.PrimaryKeySchema{
			Name: &id,
			Type: &typeInt}}
	meta := ots.TableMeta{
		TableName: "Flywire",
		SchemaEntry: schema}
	req := ots.CreateTableRequest{
		TableMeta: &meta,
		TableOption: &ots.TableOption{
			TimeToAlive: -1,
			MaxVersion: 1},
		ReservedThroughput: &ots.ReservedThroughput{
			Readcap: 0,
			Writecap: 0}}
	_, err := client.CreateTable(&req)
	if err != nil {
		panic(err)
	}
	fmt.Println("创建飞线表，成功")
}

type city struct {
	Name string
	Lat float64
	Lng float64
}

var cities = []city{
	city{
		Name: "Beijing",
		Lat: 39.92,
		Lng: 116.46},
	city{
		Name: "Shanghai",
		Lat: 31.13,
		Lng: 121.29},
	city{
		Name: "Chengdu",
		Lat: 30.67,
		Lng: 104.06},
	city{
		Name: "Shenzhen",
		Lat: 22.55,
		Lng: 114.06}}

func createOnDeliveryTable(client ots.TableStoreApi) {
	fmt.Println("创建气泡表")
	{
		id := "DestinationCity"
		typeStr := ots.PrimaryKeyType_STRING
		schema := []*ots.PrimaryKeySchema{
			&ots.PrimaryKeySchema{
				Name: &id,
				Type: &typeStr}}
		meta := ots.TableMeta{
			TableName: "Bubble",
			SchemaEntry: schema}
		req := ots.CreateTableRequest{
			TableMeta: &meta,
			TableOption: &ots.TableOption{
				TimeToAlive: -1,
				MaxVersion: 1},
			ReservedThroughput: &ots.ReservedThroughput{
				Readcap: 0,
				Writecap: 0}}
		_, err := client.CreateTable(&req)
		if err != nil {
			panic(err)
		}
	}
	{
		for _, city := range cities {
			pkc := ots.PrimaryKeyColumn{
				ColumnName: "DestinationCity",
				Value: city.Name}
			pkey := ots.PrimaryKey{
				PrimaryKeys: []*ots.PrimaryKeyColumn{&pkc}}
			lat := ots.AttributeColumn{
				ColumnName: "lat",
				Value: city.Lat}
			lng := ots.AttributeColumn{
				ColumnName: "lng",
				Value: city.Lng}
			tp := ots.AttributeColumn{
				ColumnName: "type",
				Value: int64(1)}
			val := ots.AttributeColumn{
				ColumnName: "value",
				Value: int64(0)}
			row := ots.PutRowChange{
				TableName: "Bubble",
				PrimaryKey: &pkey,
				Columns: []ots.AttributeColumn{lat, lng, tp, val},
				Condition: &ots.RowCondition{
					RowExistenceExpectation: ots.RowExistenceExpectation_IGNORE}}
			req := ots.PutRowRequest{
				PutRowChange: &row}
			_, err := client.PutRow(&req)
			if err != nil {
				panic(err)
			}
		}
	}
	fmt.Println("创建气泡表，成功")
}

func main() {
	client := newClient()

	createJournalTable(client)
	createDeliveryInfoTable(client)
	createFlywireTable(client)
	createOnDeliveryTable(client)
}
