package main
import (
	"fmt"
	"net"
	"log"
	"os"
	"strings"
	"gopkg.in/mgo.v2"
)

type StockIntraDayTrad struct {
	Code string
	Name string
	CurTime string
	Price string
	Count int
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
	Log("Waiting for clients")
	for {
		conn, err := netListen.Accept()
		if err != nil {
			continue
		}

		Log(conn.RemoteAddr().String(), " tcp connect success")
		handleConnection(conn,collection,collectionDetail)
	}
}
//处理连接
func handleConnection(conn net.Conn, collection *mgo.Collection,collectionDetail *mgo.Collection) {

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
				Name :=""
				CurTime :=resultdata[2]
				DetailCode :=resultdata[10]
				Value :=resultdata[11]
				if(len(Value)>0 && (strings.Compare(DetailCode,"151") || strings.Compare(DetailCode,"133") || strings.Compare(DetailCode,"161")){
					err = collectionDetail.Insert(&StockIntraDayTradDetail{
						Code,
						Name,
						CurTime,
						DetailCode,
						Value,
					})
					check(err)		
				}
				fmt.Printf("%q \n",resultdata)
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