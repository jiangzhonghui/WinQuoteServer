package main
import (
	"fmt"
	"net"
	"log"
	"os"
	"strings"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	//建立socket，监听端口
	netListen, err := net.Listen("tcp", "127.0.0.1:1024")
	CheckError(err)
	defer netListen.Close()



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
		con2 :=strings.Replace(string(buffer[:n]),"\n","",0);
		n2, err := f.WriteString(con2)
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