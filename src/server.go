package main
import (
	"fmt"
	"net"
	"log"
	"os"
	"strings"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

type StockIntraDayTrad struct {
	code string "bson:`code`"
	curTime string "bson:`curTime`"
	price string "bson:`price`"
	count string "bson:`count`"
}

type StockIntraLastTrad struct {
	code  string
	curTime  string
	price  string
	volumn  string
	timestamp time.Time
}

type StockIntraDayTradDetail struct {
	Code string
	Name string
	CurTime string
	DetailCode string
	Value string
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

var collection *mgo.Collection
var collectionDetail *mgo.Collection
var collectionLastTrade *mgo.Collection

func main() {

	//建立socket，监听端口
	netListen, err := net.Listen("tcp", "127.0.0.1:1024")
	CheckError(err)
	defer netListen.Close()

	//连接mongodb
	session, err :=mgo.Dial("127.0.0.1:27017")
	CheckError(err)
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	collection := session.DB("winquotedata").C("stockintradaytrad")
	collectionDetail := session.DB("winquotedata").C("stockintradaytraddetail")
	collectionLastTrade := session.DB("winquotedata").C("stockintralasttrade")

	Log("Waiting for clients")
	for {
		conn, err := netListen.Accept()
		if err != nil {
			continue
		}

		Log(conn.RemoteAddr().String(), " tcp connect success")
		handleConnection(conn,collection,collectionDetail,collectionLastTrade)
	}
}
//处理连接
func handleConnection(conn net.Conn, collection *mgo.Collection,collectionDetail *mgo.Collection,collectionLastTrade  *mgo.Collection) {

	buffer := make([]byte, 2048)
	f, err := os.Create("ts.log")
	check(err)

	defer f.Close()

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			Log(conn.RemoteAddr().String(), " connection error: ", err)
			return
		}
		Log(conn.RemoteAddr().String(), "receive data string:\n", string(buffer[:n]))
		line1 :=string(buffer[:n])
		result :=  strings.Split(line1, "\n\r")
		fmt.Println(len(result))

		for i:= range result {
			resultdata :=  strings.Split(result[i], " ")
			datalen := len(resultdata)
			if(datalen >11) {
				Code := resultdata[1]
				DetailCode :=resultdata[10]
				Value :=resultdata[11]
				CurTime :=""
				Price :=""
				Count :=""
				Number :=0
				Number, err = collectionLastTrade.Find(nil).Select(bson.M{"code": Code}).Count()
				if err != nil {
					check(err)
				}
				fmt.Println(Number)
				fmt.Println(Code)
				lastResult := StockIntraLastTrad{}
				err = collectionLastTrade.Find(nil).Select(bson.M{"code": Code}).One(&lastResult)
				if err!=nil {
					err = collectionLastTrade.Insert(&StockIntraLastTrad{
							code:Code,
							curTime:"",
							price:"",
							volumn:Value,
							timestamp: time.Now(),
						})
					check(err)
					err = collectionLastTrade.Find(nil).Select(bson.M{"code": Code}).One(&lastResult)
					check(err)
				}
				CurTime = lastResult.curTime
				fmt.Println(Code)

				if len(Value)>0 && (DetailCode=="151" || DetailCode=="133" || DetailCode=="161"){
						if(DetailCode=="151"){
							Count =Value
							lastResult.volumn = Value
						}else if(DetailCode=="133"){
							lastResult.price = Value
							Price =Value
						}else if(DetailCode=="161"){
							lastResult.curTime = Value
							CurTime =Value
						}
				}
				
				/**
				if CurTime=="" {
					lastResult := StockIntraLastTrad{}
					err = collectionLastTrade.Find(bson.M{"Code": Code).One(&lastResult)
					if err == nil {
						CurTime := lastResult ->CurTime
					}
				}else{
					lastResult := StockIntraLastTrad{}
					err = collectionLastTrade.Find(bson.M{"Code": Code).One(&lastResult)
					if err == nil {
						err = collectionLastTrade.Insert(&StockIntraLastTrad{
							Code,
							CurTime,
							Price,
							Count
						})
						check(err)	
					}else{
						
					}
				}**/

				if Price==""{
					Price = lastResult.price
				}

				selector := bson.M{"code": Code}
				change := bson.M{"$set": bson.M{"curTime": CurTime,"price": Price,"volumn": Count}}
				err := collectionLastTrade.Update(selector, change)
				if err != nil {
					//check(err)
				}
				fmt.Printf("update last intradday\n")

				fmt.Printf("lastResult.Code:%s \n",lastResult.code)
				fmt.Printf("lastResult.price:%s \n",lastResult.price)
				fmt.Printf("lastResult.curTime:%s \n",lastResult.curTime)
				fmt.Printf("lastResult.Count:%s \n",lastResult.volumn)

				if (Code!="" && CurTime!="") && (Price!="" && Count!="") {

					tradeResult := StockIntraDayTrad{}
					err = collection.Find(nil).Select(bson.M{"code": Code,"curTime": CurTime}).One(&tradeResult)
					if err != nil {
						err = collection.Insert(&StockIntraDayTrad{
							code:Code,
							curTime:CurTime,
							price:Price,
							count:Count,
						})

						err = collection.Find(nil).Select(bson.M{"code": Code,"curTime": CurTime}).One(&tradeResult)

						if(err ==nil){
							if(Price==""){
								Price = tradeResult.price
							}
							if(Count==""){
								Count = tradeResult.count
							}
						}else{
							check(err)
						}
						
					}else{
						if(Price!="" && Count!=""){
							selector := bson.M{"code": Code,"curTime": CurTime}
							data :=  bson.M{"$set": bson.M{"price": Price,"count": Count}}
							err := collection.Update(selector, data)
							if err != nil {
								check(err)
							}
							fmt.Printf("update intraday\n")
						}
					}
					
					fmt.Printf("add new intraday\n")
				}
			}
			
		}
		n2, err := f.WriteString(line1)
		check(err)
		fmt.Printf("wrote %d bytes\n", n2)
	}
	f.Sync()
}
func Log(v ...interface{}) {
	log.Println(v...)
}

func CheckError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}