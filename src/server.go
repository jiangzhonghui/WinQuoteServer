package main
import (
	"fmt"
	"net"
	"log"
	"os"
	"strings"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type StockIntraDayTrad struct {
	Code string
	Name string
	CurTime string
	Price string
	Count int
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

var collection *mgo.Collection

func main() {

	//建立socket，监听端口
	netListen, err := net.Listen("tcp", "127.0.0.1:1024")
	CheckError(err)
	defer netListen.Close()

	//连接mongodb
	session, err :=mgo.Dial("localhost:11270")
	CheckError(err)
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	collection := session.DB("winquotedata").C("stockintradaytrad")

	Log("Waiting for clients")
	for {
		conn, err := netListen.Accept()
		if err != nil {
			continue
		}

		Log(conn.RemoteAddr().String(), " tcp connect success")
		handleConnection(conn)
	}
}
//处理连接
func handleConnection(conn net.Conn) {

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
		line1 :=strings.Replace(string(buffer[:n]),"\n","",0);
		line2 :=strings.Replace(line1," ","",0);

		Code := ""
		Name :=""
		CurTime :=""
		Price :=""
		Count :=1

		err = collection.Insert(&StockIntraDayTrad{
			Code,
			Name,
			CurTime,
			Price,
			Count,
		})
		check(err)

		n2, err := f.WriteString(line2)
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