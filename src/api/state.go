// api for monitor

package api


/*
import "github.com/shirou/gopsutil"
import "github.com/go-redis/redis"


r := redis.NewClient(&redis.Options{
	Addr:		Redis_host,
	Password:	"",
	DB:			0,
})

*/


func index(c *gin.Context){

	c.Redirect(http.StatusMovedPermanently,"/index.html")

}

func test(c *gin.Context){

	name := c.Param("name")
	c.String(http.StatusOK, "Hello %s", name)
}

func sysstate(c *gin.Context):
	
/*
	if request.method == 'GET' :{

		#REDIS DB  1G
		db := Filesize(Data_dir + 'redis.rdb') / (1024*1024*1024*1) * 100)

		
		#CPU
		cpus, _:= cpu.Percent(time.Second, false)
		cpu := int(cpu[0])
		
		#memory
		memInfo, _ := mem.VirtualMemory()
		memory :=  int(memInfo.UsedPercent)
		
		#systemdisk
		diskInfo, _ := disk.Usage(System_disk)
		systemdisk :=int(diskInfo.UsedPercent) 
		
		#datadisk
		diskInfo, _ := disk.Usage(Data_disk)
		datadisk := int(diskInfo.UsedPercent)

		#GPU
		gpu := 90
		
		#temp
		temp := 60
		
			
		data  = {
					'db': db,
					'cpu': cpu,
					'memory': memory,
					'gpu': gpu,
					'systemdisk': systemdisk,
					'datadisk': datadisk,
					'temp': temp,
				}
		
	}
	return response(200, "ok", data)
	*/
}