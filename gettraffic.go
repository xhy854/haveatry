package main

import(
      "fmt"
      "net/http"
      "strings"
      "database/sql"
      _ "github.com/go-sql-driver/mysql"
      "strconv"
//    "reflect"
      "sync"
      "runtime"
      "net"
      "time"
      "regexp"
      "encoding/json"
      "bytes"
)

var count int =0;

func sendhttp(url string,state string,leng int, client *http.Client) {

        j1 := make(map[string]interface{})
        j1["url"] = url
        j1["state"] = state
        j1["contentlength"] = leng

        js1, _ := json.Marshal(j1)
        body := bytes.NewBuffer([]byte(js1))
        resp,err:=client.Post("http://localhost:5566/testmysql", "application/json;charset=utf-8", body)
        if err != nil {
            return
        }
        defer resp.Body.Close()


}

func get_head(url string,lock *sync.Mutex,client *http.Client) {
   var newurl string
   newurl="http://"+url
//   fmt.Println(newurl)

   req,err := http.NewRequest("HEAD",newurl,nil)
//   fmt.Println(err)   
   if err != nil {
   return
   }
   
   req.Header.Add("Connection", "close")

   resp,err:= client.Do(req)
   

   if err != nil || !(resp.StatusCode == http.StatusOK || resp.StatusCode == 206) {
        
        sendhttp(url,"NOTACCESS",0,client)
        lock.Lock()
        count++
        lock.Unlock()
        return
   }

   defer resp.Body.Close()

//   fmt.Println(resp.StatusCode)
//   fmt.Println(resp)
     var cachestatus bool = true
     var contentlength int
     _,iflength := resp.Header["Content-Length"]
    if iflength {
      contentlength,err = strconv.Atoi(resp.Header["Content-Length"][0])
      if err !=nil {
        fmt.Println("Convert error")
      }
     } else {
     contentlength = -1
   }
// IF CODE IS 206 , HANDLE RANGE
    _,ifrange := resp.Header["Content-Range"]
    if ifrange {
      lengthstring:=resp.Header["Content-Range"][0]
      reg := regexp.MustCompile(`(\d)+`)
      cleng := reg.FindAllString(lengthstring,2)
      if len(cleng)==2 {
         start:=cleng[0]
         end:=cleng[1]
         endnum,_:= strconv.Atoi(end)
         startnum,_:= strconv.Atoi(start)
         contentlength = endnum-startnum
      } else {
      contentlength =-2
      }
   }

   cache := resp.Header["Cache-Control"]
   for _,v := range cache {
   if strings.Contains(v,"private") || strings.Contains(v,"no-store") || strings.Contains(v,"no-cache") {
   cachestatus= false
   break
   }
   }

//   fmt.Println(cachestatus)
//   fmt.Println(contentlength)

   if cachestatus {
      sendhttp(url,"CACHE",contentlength,client)
   } else {
      sendhttp(url,"NOCACHE",contentlength,client)
   }

   lock.Lock()
   count++
   lock.Unlock()
   return
}


func sendrequest(start int,interval int,db *sql.DB,client *http.Client) {
    
    rows,err := db.Query("select * from url_stat limit ?,?",start,interval)
    CheckErr(err)
    defer rows.Close()

    var domain,url,state,ts string
    var num,leng int
    lock:= &sync.Mutex{}
    var url_total=0


    for rows.Next() {
    err= rows.Scan(&domain,&url,&num,&state,&leng,&ts)
    CheckErr(err)
    go get_head(url,lock,client)
    url_total++
    }
    fmt.Println("TOTAL NUM")
    fmt.Println(url_total)

    for {
       lock.Lock()
       c:=count
       lock.Unlock()
       runtime.Gosched()
       if c>=url_total{
                  fmt.Println("goroutine end")
                  break
       }

      }

    return
}

func main() {
   t1 := time.Now().Unix()    

   client := &http.Client{
        Transport: &http.Transport{
        Dial: func(netw, addr string) (net.Conn, error) {
            c, err := net.DialTimeout(netw, addr, time.Second*10)
            if err != nil {
                fmt.Println("dail timeout", err)
                return nil, err
            }
            return c, nil
        },
        MaxIdleConnsPerHost:   10,
        ResponseHeaderTimeout: time.Second * 10,
      },
   }

    db, _ := sql.Open("mysql", "root:db10$ZTE@tcp(localhost:5518)/traffic_stat")
    defer db.Close()
    var interval int = 2000
  
    var totalnumber int
    countrows,err:=db.Query("select count(*) from url_stat")
    CheckErr(err)
    countrows.Next()
    countrows.Scan(&totalnumber)
    fmt.Println(totalnumber)
    var times int = totalnumber/interval
    var last int = totalnumber%interval
    for i:=0;i< times; i++ {
     sendrequest(i*interval,interval,db,client)
     count=0
    }

  // sendrequest(50000,1000,db,client)
   sendrequest(interval*times,last,db,client)
   fmt.Println("All URLs finish!") 
   t2 := time.Now().Unix()
   fmt.Println("---TIME RUNNING THIS PROGRAM---")
   fmt.Println(t2-t1)
}


func CheckErr(err error) {
 if err !=nil {
     panic(err)
}
}
