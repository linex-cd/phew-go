package api

//api for monitor

import "time"

import "net/http"
import "github.com/gin-gonic/gin"

import "github.com/shirou/gopsutil/cpu"
import "github.com/shirou/gopsutil/mem"
import "github.com/shirou/gopsutil/disk"

//import "github.com/go-redis/redis"


/*
var r *redis.Client

func init(){
	r := redis.NewClient(&redis.Options{
		Addr:		Redis_host,
		Password:	"",
		DB:			0,
	})
	

}
*/


func index(c *gin.Context){

	c.Redirect(http.StatusMovedPermanently,"/index.html")

}

func test(c *gin.Context){

	name := c.Param("name")
	c.String(http.StatusOK, "Hello %s", name)
}

func sysstate(c *gin.Context){
	


		//REDIS DB  1G
		db := int(Filesize(Data_dir + "redis.rdb") / (1024*1024*1024*1) * 100)

		
		//CPU
		percent, _ := cpu.Percent(time.Second, false)
		cpu := int(percent[0])
		
		//memory
		memInfo, _ := mem.VirtualMemory()
		memory :=  int(memInfo.UsedPercent)
		
		//systemdisk
		sysdiskInfo, _ := disk.Usage(System_disk)
		systemdisk :=int(sysdiskInfo.UsedPercent) 
		
		//datadisk
		datadiskInfo, _ := disk.Usage(Data_disk)
		datadisk := int(datadiskInfo.UsedPercent)

		//GPU
		gpu := 90
		
		//temp
		temp := 60
		
			
		data  := map[string]int{
					"db": db,
					"cpu": cpu,
					"memory": memory,
					"gpu": gpu,
					"systemdisk": systemdisk,
					"datadisk": datadisk,
					"temp": temp,
				}
		
	
	ResponseJson(c, 200, "ok", data)
	
}