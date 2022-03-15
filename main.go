package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/joeqian10/neo3-gogogo/crypto"
	"github.com/joeqian10/neo3-gogogo/helper"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path/filepath"
)
const (
	OCTOBER_START_TIME int64 = 1633046400000
	NOMVEMBER_START_TIME int64 = 1635724800000
	DECEMBER_START_TIME int64 = 1638316800000
	JANUARY_START_TIME int64 = 1640995200000
	FEBRUARY_START_TIME int64 = 1643673600000
	MARCH_START_TIME int64= 1646092800000
)

type Config struct {
	Database_main struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		User     string `yaml:"user"`
		Pass     string `yaml:"pass"`
		Database string `yaml:"database"`
		DBName   string `yaml:"dbname"`
	} `yaml:"database_main"`
	Database_test struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		User     string `yaml:"user"`
		Pass     string `yaml:"pass"`
		Database string `yaml:"database"`
		DBName   string `yaml:"dbname"`
	} `yaml:"database_test"`
}

type AddressTransfer struct {
	Address string `json:"_id"`
	Count int
	TotalPrice string
}
type AddressTransaction struct {
	Address string `json:"_id"`
	Count int
}
type Transaction struct {
	Hash string `json:"hash"`
}
type Contract struct {
	Contract string `json:"_id"`
	Count int
}
type ScCall struct {
	Txid string `json:"txid"`
	ContractHash string `json:"contractHash"`
	Method string `json:"method"`
}
func main() {
	cfg, err := OpenConfigFile()
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.TODO()
	db,dbName := initializeMongoOnlineClient(cfg, ctx)

	//tx_october,_,tx3_oct,_,err :=getSummary(db,dbName,ctx,OCTOBER_START_TIME,NOMVEMBER_START_TIME)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//
	//tx_november,_,tx3_nov,_,err :=getSummary(db,dbName,ctx,NOMVEMBER_START_TIME,DECEMBER_START_TIME)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//
	//tx_december,_,tx3_dec,_,err :=getSummary(db,dbName,ctx,DECEMBER_START_TIME,JANUARY_START_TIME)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//
	//tx_january,_,tx3_jan,_,err :=getSummary(db,dbName,ctx,JANUARY_START_TIME,FEBRUARY_START_TIME)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//
	//tx_february,_,tx3_february,_,err :=getSummary(db,dbName,ctx,FEBRUARY_START_TIME,MARCH_START_TIME)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//fmt.Println("===============================================")
	//fmt.Println("October tx counts: ",tx_october)
	////fmt.Println("October activeAddress counts: ",addr_october)
	//fmt.Println("October Transaction > 3 address counts: ",tx3_oct)
	////fmt.Println("October NEO >1 || GAS >1 address counts: ",neogas1_oct)
	//fmt.Println("===============================================")
	//fmt.Println("November tx counts: ",tx_november)
	////fmt.Println("November activeAddress counts: ",addr_november)
	//fmt.Println("November Transaction > 3 address counts: ",tx3_nov)
	////fmt.Println("November NEO >1 || GAS >1 address counts: ",neogas1_nov)
	//fmt.Println("===============================================")
	//fmt.Println("December tx counts: ",tx_december)
	////fmt.Println("December activeAddress counts: ",addr_december)
	//fmt.Println("December Transaction > 3 address counts: ",tx3_dec)
	////fmt.Println("December NEO >1 || GAS >1 address counts: ",neogas1_dec)
	//fmt.Println("===============================================")
	//fmt.Println("January tx counts: ",tx_january)
	////fmt.Println("January activeAddress counts: ",addr_january)
	//fmt.Println("January Transaction > 3 address counts: ",tx3_jan)
	////fmt.Println("January NEO >1 || GAS >1 address counts: ",neogas1_jan)
	//fmt.Println("===============================================")
	//fmt.Println("February tx counts: ",tx_february)
	////fmt.Println("February activeAddress counts: ",addr_february)
	//fmt.Println("February Transaction > 3 address counts: ",tx3_february)
	////fmt.Println("February NEO >1 || GAS >1 address counts: ",neogas1_february)
	//fmt.Println("===============================================")
	res, err := getMonthlyTransaction(db,dbName,ctx,FEBRUARY_START_TIME,MARCH_START_TIME)
	fmt.Println(res)
	getTransactionContract(db,dbName,ctx,FEBRUARY_START_TIME,MARCH_START_TIME)
	//test(db,dbName,ctx)


}

func test (db *mongo.Client,dbName string, ctx context.Context,){
	filter := bson.M{"txid": "0xa10d030d4966adcfe28bc352894efbfedd09bdaa263b5a0e045cb6236d3f00d5"}
	res := db.Database(dbName).Collection("ScCall").FindOne(ctx,filter)
	bytes, err := res.DecodeBytes()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(gjson.Get(bytes.String(),"contractHash"))
	fmt.Println(res.DecodeBytes())

	fmt.Println(res)
}
func getSummary (db *mongo.Client,dbName string, ctx context.Context,from int64, to int64) (int64,int,int,int,error){
	monthlyTxCount, err := getMonthlyTransaction(db,dbName,ctx,from,to)
	if err != nil {
		fmt.Println(err)
		return -1,-1,-1,-1,err
	}
	addressTransaction,addressTransactionCount,err := getActiveAddressTransaction(db,dbName,ctx,from,to)

	fmt.Println(addressTransaction)
	if err != nil {
		fmt.Println(err)
		return -1,-1,-1,-1,err
	}
	addressTransfers,addressTransfersCount,err := getActiveAddressTransfers(db,dbName,ctx,from,to)
	if err != nil {
		fmt.Println(err)
		return -1,-1,-1,-1,err
	}
	fmt.Println(addressTransfers)

	var monthlyActiveCount = 0
	for i := 0; i < len(addressTransaction); i++ {
		if _,ok := addressTransfers[addressTransaction[i].Address];ok{
			monthlyActiveCount = monthlyActiveCount +1
		}
	}
	fmt.Println("Transaction > 3 counts:",addressTransactionCount)
	fmt.Println("NEO >1 || GAS >1 counts:",addressTransfersCount)
	fmt.Println("Satisfy standard:",monthlyActiveCount)
	return monthlyTxCount, monthlyActiveCount,addressTransactionCount,addressTransfersCount,nil
}

func getMonthlyTransaction(db *mongo.Client,dbname string, ctx context.Context,from int64, to int64) (int64,error) {
	filter := bson.M{"blocktime": bson.M{"$gte": from,"$lt":to}}
	count, err := db.Database(dbname).Collection("Transaction").CountDocuments(ctx,filter)
	if err != nil {
		fmt.Println(err)
		return -1 ,err
	}
	return count, nil

}
func getTransactionContract(db *mongo.Client,dbname string, ctx context.Context,from int64, to int64) ([]Transaction,int,error) {
	var results []map[string]interface{}
	var txContract = make(map[string]int)
	pipline := []bson.M{
		bson.M{"$match":
			bson.M{"blocktime": bson.M{"$gte": from,"$lt":to},
			},
		},
		//bson.M{"$lookup":bson.M{
		//	"from":"ScCall",
		//	"let": bson.M{"txid":"$hash"},
		//	"pipeline": []bson.M{
		//		bson.M{"$match": bson.M{"$expr": bson.M{"$and": []interface{}{
		//			bson.M{"$eq": []interface{}{"$txid", "$$txid"}},
		//		}}}},
		//		bson.M{"$project": bson.M{"contractHash":1}},
		//	},
		//	"as" :"properties",
		//}},
		bson.M{"$lookup":bson.M{
				"from" : "ScCall",
				"localField":"hash",
				"foreignField":"txid",
				"as":"info",
			},
		},
		bson.M{"$unwind":bson.M{"path":"$info","preserveNullAndEmptyArrays":true}},
		bson.M{"$project": bson.M{"info.contractHash":1,"info.txid":1}},
		bson.M{"$group":bson.M{"_id":"$info.txid","contractHash":bson.M{"$first":"$info.contractHash"}}},
		bson.M{"$group":bson.M{"_id":"$contractHash","count":bson.M{"$sum":1}}},

	}
	cursor, err := db.Database(dbname).Collection("Transaction").Aggregate(ctx,pipline)
	if err == mongo.ErrNoDocuments {
		fmt.Println(err)
		return nil, -1,err
	}
	if err != nil {
		fmt.Println(err)
		return nil, -1,err
	}
	if err = cursor.All(ctx, &results); err != nil {
		fmt.Println(err)
		return nil, -1,err
	}
	if err != nil {
		fmt.Println(err)
		return nil,-1, err
	}
	defer func(cursor *mongo.Cursor, ctx context.Context) {
		err := cursor.Close(ctx)
		if err != nil {
			fmt.Println("Closing cursor error")
		}
	}(cursor, ctx)
	//fmt.Println(results)
	resultBytes ,err := json.Marshal(results)
	if err != nil {
		fmt.Println(err)
		return nil, -1,err
	}
	var contracts []Contract
	err = json.Unmarshal([]byte(resultBytes), &contracts)
	if err != nil {
		fmt.Println(err)
		return nil, -1,err
	}
	fmt.Println(contracts)
	for i := 0; i < len(contracts); i++ {
		if checkContractType(contracts[i].Contract) == "NEO/GAS" {
			txContract["NEO/GAS"] = txContract["NEO/GAS"] + contracts[i].Count
		}else if checkContractType(contracts[i].Contract) == "GM" {
			txContract["GM"] = txContract["GM"] + contracts[i].Count
		} else if checkContractType(contracts[i].Contract) == "FLM" {
			txContract["FLM"] = txContract["FLM"] + contracts[i].Count
		} else {
			txContract["OTHER"] = txContract["OTHER"] + contracts[i].Count
		}
	}
	//fmt.Println(scCall)
	fmt.Println(txContract)
	return nil, 1, nil

}
func checkContractType (contractHash string ) string{


	if contractHash == "0xef4073a0f2b305a38ec4050e4d3d28bc40ea63f5" || contractHash == "0xd2a4cff31913016155e38e474a2c06d08be276cf"{
		return "NEO/GAS"
	}else if contractHash == "0xcc638d55d99fc81295daccbaf722b84f179fb9c4" || contractHash == "0x9b049f1283515eef1d3f6ac610e1595ed25ca3e9" || contractHash == "0x577a51f7d39162c9de1db12a6b319c848e4c54e5" {
		return "GM"
	} else if contractHash == "0xf970f4ccecd765b63732b821775dc38c25d74f23" || contractHash == "0xf0151f528127558851b39c2cd8aa47da7418ab28" || contractHash == "0xd1a9f78e1940f6322fef4df2340a963a9ec46f63" ||
		contractHash == "0xa35233a13081335bec2b56db16903b07f07bf1a6" || contractHash == "0x545dee8354823d1bdf4ac524e4092f7405025247" || contractHash == "0x3244fcadcccff190c329f7b3083e4da2af60fbce" ||
		contractHash == "0x4d5a85b0c83777df72cfb665a933970e4e20c0ec" || contractHash == "0x1404929a660620869c9cb46ff228ee9d7147959d" || contractHash == "0xc777a8032c1d9d7b885c7357d4c93e7a39f93942" ||
		contractHash == "0xedcbe55b04bcc7dad69cfe243bf3d26dc106a1d4" || contractHash == "0x59aa80468a120fe79aa5601de07746275c9ed76a" || contractHash == "0x6bcbf09a7193c968d608178a45785967f0721c42" ||
		contractHash == "0x171d791c0301c332cfe95c6371ee32965e34b606" || contractHash == "0x1b3f740240af479f07e44ee3ee78df4c6cb4b1fb" || contractHash == "0x45d182227b5d753c7f358594b631838b92caf409" ||
		contractHash == "0xf23221a92c29beffbea6e46c681c8380d9794579" || contractHash == "0x236a6679dc26b5f11fae7c3b30784509216dd4b0"{
		return "FLM"
	} else {
		return "OTHER"
	}
}
func getActiveAddressTransaction(db *mongo.Client,dbname string, ctx context.Context,from int64, to int64) ([]AddressTransaction,int,error) {
	var results []map[string]interface{}
	pipline := []bson.M{
		bson.M{"$match": bson.M{"blocktime": bson.M{"$gte": from,"$lt":to}}},
		bson.M{"$group":bson.M{"_id":"$sender","count":bson.M{"$sum":1}}},
		bson.M{"$match": bson.M{"count": bson.M{"$gt": 3}}},
		//bson.M{"$group":bson.M{"_id":"null","count":bson.M{"$sum":1}}},
	}
	cursor, err := db.Database(dbname).Collection("Transaction").Aggregate(ctx,pipline)
	if err == mongo.ErrNoDocuments {
		return nil, -1,err
	}
	if err != nil {
		return nil, -1,err
	}
	if err = cursor.All(ctx, &results); err != nil {
		return nil,-1, err
	}
	if err != nil {
		return nil,-1, err
	}
	defer func(cursor *mongo.Cursor, ctx context.Context) {
		err := cursor.Close(ctx)
		if err != nil {
			fmt.Println("Closing cursor error")
		}
	}(cursor, ctx)
	resultBytes ,err := json.Marshal(results)
	if err != nil {
		fmt.Println(err)
		return nil, -1,err
	}
	var addressTransaction []AddressTransaction
	err = json.Unmarshal([]byte(resultBytes), &addressTransaction)
	if err != nil {
		fmt.Println(err)
		return nil, -1,err
	}
	for i := 0; i < len(addressTransaction); i++ {
		addressHash, err := crypto.AddressToScriptHash(addressTransaction[i].Address,helper.DefaultAddressVersion)
		if err != nil {
			fmt.Println(err)
			return nil,-1, err
		}
		addressTransaction[i].Address = "0x"+addressHash.String()
	}
	return addressTransaction,len(addressTransaction) ,nil
}

func getActiveAddressTransfers(db *mongo.Client,dbName string, ctx context.Context,from int64, to int64 ) (map[string]string,int, error){
	var resultAddress = make(map[string]string)
	gasAddress,err := getTokenTransfer(db,dbName,ctx,from,to,"0xd2a4cff31913016155e38e474a2c06d08be276cf",100000000)
	if err != nil {
		fmt.Println(err)
		return nil, -1,err
	}
	gasAddressBytes ,err := json.Marshal(gasAddress)
	if err != nil {
		fmt.Println(err)
		return nil,-1, err
	}
	//fmt.Println(string(gasAddressBytes))
	var addressGasTransfers []AddressTransfer
	err = json.Unmarshal([]byte(gasAddressBytes), &addressGasTransfers)
	if err != nil {
		fmt.Println(err)
		return nil,-1, err
	}
	fmt.Println(addressGasTransfers)
	for i := 0; i < len(addressGasTransfers); i++ {
		resultAddress[addressGasTransfers[i].Address] = "gas"
	}

	neoAddress,err := getTokenTransfer(db,dbName,ctx,from,to,"0xef4073a0f2b305a38ec4050e4d3d28bc40ea63f5",1)
	if err != nil {
		fmt.Println(err)
		return nil, -1,err
	}
	neoAddressBytes ,err := json.Marshal(neoAddress)
	if err != nil {
		fmt.Println(err)
		return nil, -1,err
	}

	//fmt.Println(string(neoAddressBytes))
	var addressNeoTransfers []AddressTransfer
	err = json.Unmarshal([]byte(neoAddressBytes), &addressNeoTransfers)
	if err != nil {
		fmt.Println("some error")
		return nil,-1, err
	}
	fmt.Println(addressNeoTransfers)
	for i := 0; i < len(addressNeoTransfers); i++ {
		if _,ok := resultAddress[addressNeoTransfers[i].Address];ok{
			resultAddress[addressNeoTransfers[i].Address] = "gas-neo"
		} else {
			resultAddress[addressNeoTransfers[i].Address] = "neo"
		}

	}
	return  resultAddress ,len(resultAddress),nil



}
func getTokenTransfer(db *mongo.Client,dbname string, ctx context.Context,from int64, to int64,token string,comp int) ([]map[string]interface{},error,) {
	var results []map[string]interface{}
	pipline := []bson.M{
		bson.M{"$match":
			bson.M{"timestamp": bson.M{"$gte": from,"$lt":to},
					"contract":token,
					"from":bson.M{"$ne":nil},
					"txid": bson.M{"$ne":"0x0000000000000000000000000000000000000000000000000000000000000000"},
				},
			},
		bson.M{"$group":bson.M{"_id":"$from","count":bson.M{"$sum":1},"totalPrice":bson.M{"$sum":"$value"}}},
		bson.M{"$match": bson.M{"totalPrice": bson.M{"$gte": comp}}},
		//bson.M{"$group":bson.M{"_id":"null","count":bson.M{"$sum":1}}},
	}
	cursor, err := db.Database(dbname).Collection("TransferNotification").Aggregate(ctx,pipline)
	if err == mongo.ErrNoDocuments {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	defer func(cursor *mongo.Cursor, ctx context.Context) {
		err := cursor.Close(ctx)
		if err != nil {
			fmt.Println("Closing cursor error")
		}
	}(cursor, ctx)
	return results, nil
}


func OpenConfigFile() (Config, error) {
	absPath, _ := filepath.Abs("config.yml")
	f, err := os.Open(absPath)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	var cfg Config
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		return Config{}, err
	}
	return cfg, err
}
func initializeMongoOnlineClient(cfg Config, ctx context.Context) (*mongo.Client, string) {
	rt := os.ExpandEnv("${RUNTIME}")
	var clientOptions *options.ClientOptions
	var dbOnline string
	if rt != "mainnet" && rt != "testnet" {
		rt = "mainnet"
	}
	switch rt {
	case "mainnet":
		clientOptions = options.Client().ApplyURI("mongodb://" + cfg.Database_main.User + ":" + cfg.Database_main.Pass + "@" + cfg.Database_main.Host + ":" + cfg.Database_main.Port + "/" + cfg.Database_main.Database)
		dbOnline = cfg.Database_main.Database
	case "testnet":
		clientOptions = options.Client().ApplyURI("mongodb://" + cfg.Database_test.User + ":" + cfg.Database_test.Pass + "@" + cfg.Database_test.Host + ":" + cfg.Database_test.Port + "/" + cfg.Database_test.Database)
		dbOnline = cfg.Database_test.Database
	}

	clientOptions.SetMaxPoolSize(50)
	co, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal("momgo connect error")
	}
	err = co.Ping(ctx, nil)
	if err != nil {
		log.Fatal("ping mongo error")
	}
	fmt.Println("Connect mongodb success")
	return co, dbOnline
}
