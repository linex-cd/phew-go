function showjob(job_access_key){
	$.get("state/peekjob/?job_access_key="+job_access_key, function(ret){
		if (ret['code'] != 200){
			alert(ret['msg'])
		}
		else{
			var data = ret['data'];
			
			var create_time = data['create_time'];
			if (create_time != '' ){
				var time = new Date()
				time.setTime(parseInt(create_time)*1000)
				create_time = time.toLocaleString()
			}
			var finish_time = data['finish_time'];
			if (finish_time != '' ){
				var time = new Date()
				time.setTime(parseInt(finish_time)*1000)
				finish_time = time.toLocaleString()
			}
			
			var text = 'ID：' + data['job_id'] + '\n';
			text = text + 'State: ' + data['state'] + '\n';
			text = text + 'Create Time: ' + create_time + '\n';
			text = text + 'Finish Time: ' + finish_time + '\n';
			text = text + 'Vendor ID: ' + data['vendor_id'] + '\n';
			text = text + 'Worker Group: ' + data['worker_group'] + '\n';
			text = text + 'Description: ' + data['description'] + '\n';
			text = text + 'Priority: ' + data['priority'] + '\n';
			text = text + 'Length: ' + data['length'] + '\n';
			text = text + 'Meta: ' + data['meta'] + '\n';
			alert(text)
		}
		
	});  
	
}

function showtask(task_access_key){
	$.get("state/peektask/?task_access_key="+task_access_key, function(ret){
		if (ret['code'] != 200){
			alert(ret['msg'])
		}
		else{
			var data = ret['data'];
			
			var create_time = data['create_time'];
			if (create_time != '' ){
				var time = new Date()
				time.setTime(parseInt(create_time)*1000)
				create_time = time.toLocaleString()
			}
			var start_time = data['start_time'];
			if (start_time != '' ){
				var time = new Date()
				time.setTime(parseInt(start_time)*1000)
				start_time = time.toLocaleString()
			}
			var finish_time = data['finish_time'];
			if (finish_time != '' ){
				var time = new Date()
				time.setTime(parseInt(finish_time)*1000)
				finish_time = time.toLocaleString()
			}
			
			var text = 'ID：' + data['job_id'] + '\n';
			text = text + 'State: ' + data['state'] + '\n';
			text = text + 'Note: ' + data['note'] + '\n';
			text = text + 'Create Time: ' + create_time + '\n';
			text = text + 'Start Time: ' + start_time + '\n';
			text = text + 'Finish Time: ' + finish_time + '\n';
			text = text + 'Priority: ' + data['priority'] + '\n';
			text = text + 'Addressing: ' + data['addressing'] + '\n';
			text = text + 'Port: ' + data['port'] + '\n';
			text = text + 'Meta: ' + data['meta'] + '\n';
			alert(text)
		}
		
	});  
	
	
}

(function ($) {
 "use strict";
		
		/////////////////////////////////////////////////
		
		$.get("state/sysstate", function(ret){
			$("#db").attr('data-rel', ret['data']['db']);
			$("#cpu").attr('data-rel', ret['data']['cpu']);
			$("#gpu").attr('data-rel', ret['data']['gpu']);
			$("#systemdisk").attr('data-rel', ret['data']['systemdisk']);
			$("#datadisk").attr('data-rel', ret['data']['datadisk']);
			$("#temp").attr('data-rel', ret['data']['temp']);
			
			//knob
			if(typeof($.fn.knob) != 'undefined') {
				$('.knob').each(function () {
				  var $this = $(this),
					  knobVal = $this.attr('data-rel');
			
				  $this.knob({
					'draw' : function () { 
					  $(this.i).val(this.cv + '%')
					}
				  });
				  
				  $this.appear(function() {
					$({
					  value: 0
					}).animate({
					  value: knobVal
					}, {
					  duration : 1000,
					  easing   : 'swing',
					  step     : function () {
						$this.val(Math.ceil(this.value)).trigger('change');
					  }
					});
				  }, {accX: 0, accY: -150});
				});
			}	
			
			
		});  
		
		/////////////////////////////////////////////////
		var isloading_jobcounter = false;
		function load_jobconter(){
			if(isloading_jobcounter){
				return ;
			}
			isloading_jobcounter = true;

			$.get("state/jobcounter", function(ret){
				$("#job_total").text(ret['data']['job_total']);
				$("#task_total").text(ret['data']['task_total']);
				$("#job_pending").text(ret['data']['job_pending']);
				$("#work_pending").text(ret['data']['work_pending']);
				
				isloading_jobcounter = false;
			});
		}
		
		load_jobconter();
		setInterval(function(){
			load_jobconter();
			
		}, 5000);
		
		/////////////////////////////////////////////////
		var isloading_latestwork = false;
		function load_latestwork(){
			if(isloading_latestwork){
				return ;
			}
			isloading_latestwork = true;
			
			$.get("state/latestwork", function(ret){
				
				//////////////
				var latest_job_htmlstr = ''
				var job_latest = ret['data']['job_latest']
				var len = job_latest.length
				for(var i = 0; i < len; i++){
					var item = job_latest[i]
					var time = new Date();
					time.setTime(parseInt(item[0])*1000);
					var ts = time.toLocaleString();
					var priority = item[3];
					if (parseInt(priority) > 5)
					{
						priority = priority + "*";
					}
					var str = '<tr>\
									<td class="f-500 c-cyan">'+item[2]+'</td>\
									<td ><a href="javascript:void(0);" onclick="showjob(\''+item[5]+'\')">['+priority+']'+item[4]+'</a>&nbsp;&nbsp;&nbsp;<span style="color:gray;font-size:12px">'+ts+'<span></td>\
									<td class="f-500 c-cyan">'+item[1]+'</td>\
								</tr>'
					latest_job_htmlstr = str + latest_job_htmlstr
				}
				
				$('#latest_job').html(latest_job_htmlstr);
				
				//////////////
				var latest_task_htmlstr = ''
				var task_latest = ret['data']['task_latest']
				var len = task_latest.length
				for(var i = 0; i < len; i++){
					var item = task_latest[i]
					
					var str = '<div class="recent-post-flex rct-pt-mg">\
										<div class="recent-post-img" style="width:72px;">\
											<a href="state/peekfile/?filename='+item['data']+'" target="_blank"><img src="img/'+item['port']+'.png" alt="Click to download file" /></a>\
										</div>\
										<div class="recent-post-it-ctn">\
											<a href="javascript:void(0);" onclick="showjob(\''+item['job_access_key']+'\')"><h5>'+item['description']+'</h5></a>\
											<a href="javascript:void(0);" onclick="showtask(\''+item['task_access_key']+'\')"><p title="'+item['data']+'" >'+item['data']+'</p></a>\
										</div>\
								</div>'
					latest_task_htmlstr = str + latest_task_htmlstr
				}
				if (latest_task_htmlstr == ''){
					latest_task_htmlstr = '<div>No Task on Working<div>'
				}
				
				
				$('#latest_task').html(latest_task_htmlstr);
				
				//////////////
				isloading_latestwork = false;
				
			});  
		}
		
		load_latestwork();
		setInterval(function(){		
			load_latestwork();
		}, 5000);
		
		/////////////////////////////////////////////////
		$.get("state/errorlist", function(ret){
				
			//////////////
			var error_job_htmlstr = ''
			var error_jobs = ret['data']['error_jobs']
			var len = error_jobs.length
			for(var i = 0; i < len; i++){
				var item = error_jobs[i];
				var time = new Date();
				time.setTime(parseInt(item[0])*1000);
				var ts = time.toLocaleString();
				var priority = item[3];
				if (parseInt(priority) > 5)
				{
					priority = priority + "*";
				}
				var str = '<tr>\
								<td class="f-500 c-cyan">'+item[2]+'</td>\
								<td ><a href="javascript:void(0);" onclick="showjob(\''+item[5]+'\')">['+priority+']'+item[4]+'</a>&nbsp;&nbsp;&nbsp;<span style="color:gray;font-size:12px">'+ts+'<span></td>\
								<td class="f-500 c-cyan">'+item[1]+'</td>\
							</tr>'
				error_job_htmlstr = str + error_job_htmlstr
			}
			
			$('#error_jobs').html(error_job_htmlstr);
			
			//////////////
			var error_task_htmlstr = ''
			var error_tasks = ret['data']['error_tasks']
			var len = error_tasks.length
			for(var i = 0; i < len; i++){
				var item = error_tasks[i]
				
				var str = '<div class="recent-post-flex rct-pt-mg">\
									<div class="recent-post-img" style="width:72px;">\
										<a href="state/peekfile/?filename='+item['data']+'" target="_blank"><img src="img/'+item['port']+'.png" alt="Open the File" /></a>\
									</div>\
									<div class="recent-post-it-ctn">\
										<a href="javascript:void(0);" onclick="showjob(\''+item['job_access_key']+'\')"><h5>'+item['description']+'</h5></a>\
										<a href="javascript:void(0);" onclick="showtask(\''+item['task_access_key']+'\')"><p title="'+item['data']+'" >'+item['data']+'</p></a>\
									</div>\
							</div>'
				error_task_htmlstr = str + error_task_htmlstr
			}
			if (error_task_htmlstr == ''){
				error_task_htmlstr = '<div>No Error Task<div>'
			}
			
			
			$('#error_tasks').html(error_task_htmlstr);
			
	
		});  
	
		/////////////////////////////////////////////////
		$.get("state/percentage", function(ret){
				
			//////////////
			var percentagepie = echarts.init(document.getElementById('percentage-pie'));
			
			///
			var ports = ret['data']['port']

			var keys = Object.keys(ports);
			
			var port_data = []
			
			for (var i = 0; i < keys.length; i++)
			{
				var item = {};
				item['name'] = keys[i]
				item['value'] = ports[keys[i]]
				port_data.push(item);
			}
			
			///
			var addressings = ret['data']['addressing']

			var keys = Object.keys(addressings);
			
			var addressing_data = []
			
			for (var i = 0; i < keys.length; i++)
			{
				var item = {};
				item['name'] = keys[i]
				item['value'] = addressings[keys[i]]
				addressing_data.push(item);
			}
			
			var option = {

				tooltip: {
					trigger: 'item',
					formatter: '{a} <br/>{b} : {c} ({d}%)'
				},

				series: [
					{
						name: 'port',
						type: 'pie',
						radius: '50%',
						center: ['45%', '30%'],
						data: port_data,
						emphasis: {
							itemStyle: {
								shadowBlur: 10,
								shadowOffsetX: 0,
								shadowColor: 'rgba(0, 0, 0, 0.5)'
							}
						}
					},
					{
						name: 'address',
						type: 'pie',
						radius: '40%',
						center: ['60%', '70%'],
						data: addressing_data,
						emphasis: {
							itemStyle: {
								shadowBlur: 10,
								shadowOffsetX: 0,
								shadowColor: 'rgba(0, 0, 0, 0.5)'
							}
						}
					}
				]
			};
			
			percentagepie.setOption(option);


		});  
	
		
		/////////////////////////////////////////////////
		
		$.get("state/nodecounter", function(ret){
			$("#vendor_count").text(ret['data']['vendor_count']);
			$("#worker_count").text(ret['data']['worker_count']);
			

			
			//map count
			var workerslen_jiangsu = 0
			var workerslen_zhejiang = 0
			
			//city
			var worker_city = []
			var vendor_city = []
			
			////
			var workers_htmlstr = ''
			var workers = ret['data']['workers']
			
			var len = workers.length
			for(var i = 0; i < len; i++){
				var item = workers[i]
				
				var isonline = ''
				if (item['state']=='online'){
					isonline = 'checked'
				}
				
				var time = new Date()
				time.setTime(parseInt(item['ping_time'])*1000)
				var ts = time.toLocaleString()
				var str = '<li class="list-group-item">\
								<div class="checkbox checkbox-primary">\
									<input class="todo-done" type="checkbox" '+isonline+' readonly />\
									<label>'+item['location']+':'+item['name']+'<br/><span style="color:gray;font-size:12px">'+item['ip']+'&nbsp;&nbsp;'+ts+'</span></label>\
								</div>\
							</li>'
				workers_htmlstr = str + workers_htmlstr
				
				if (item['location'] == '南京'){
					workerslen_jiangsu = workerslen_jiangsu + 1
				}
				
				if (item['location'] == '杭州'){
					workerslen_zhejiang = workerslen_zhejiang + 1
				}
				
				//worker_city
				if (worker_city.includes(item['location']) == false){
					worker_city.push(item['location'])
				}
			}
			if (workers_htmlstr == ''){
				workers_htmlstr = '<div class="recent-post-signle">No Worker Node</div>'
			}
			
			//city tag
			var worker_city_str = ''
			for(var i = 0; i < worker_city.length; i++){

				var str = '<div class="realtime-bw">\
								<span>'+worker_city[i]+'</span>\
							</div>'
				worker_city_str = str + worker_city_str
				
			}

			$('#worker_city').html(worker_city_str);
			
			
			/////
			var vendors_htmlstr = ''
			var vendors = ret['data']['vendors']
			
			var len = vendors.length
			for(var i = 0; i < len; i++){
				var item = vendors[i]
				
				var isonline = ''
				if (item['state']=='online'){
					isonline = 'checked'
				}
				
				var time = new Date()
				time.setTime(parseInt(item['ping_time'])*1000)
				var ts = time.toLocaleString()
				var str = '<li class="list-group-item">\
								<div class="checkbox checkbox-primary">\
									<input class="todo-done" type="checkbox" '+isonline+' readonly />\
									<label>'+item['location']+':'+item['name']+'<br/><span style="color:gray;font-size:12px">'+item['ip']+'&nbsp;&nbsp;'+ts+'</span></label>\
								</div>\
							</li>'
				vendors_htmlstr = str + vendors_htmlstr
				
				//vendor_city
				if (vendor_city.includes(item['location']) == false){
					vendor_city.push(item['location'])
				}
			}
			if (vendors_htmlstr == ''){
				vendors_htmlstr = '<div class="recent-post-signle">No Vendor Node</div>'
			}
			
			$('#node-list').html(vendors_htmlstr+workers_htmlstr);
			
			//city tag
			var vendor_city_str = ''
			for(var i = 0; i < vendor_city.length; i++){

				var str = '<div class="realtime-bw">\
								<span>'+vendor_city[i]+'</span>\
							</div>'
				vendor_city_str = str + vendor_city_str
				
			}

			$('#vendor_city').html(vendor_city_str);
			
			
			//map
			var cityName = [
					{id: "CN-32", name: "江苏",},
					{id: "CN-52", name: "贵州",},
					{id: "CN-53", name: "云南",},
					{id: "CN-50", name: "重庆",},
					{id: "CN-51", name: "四川",},
					{id: "CN-31", name: "上海",},
					{id: "CN-54", name: "西安",},
					{id: "CN-33", name: "浙江",},
					{id: "CN-15", name: "内蒙古"},
					{id: "CN-14", name: "山西",},
					{id: "CN-",   name: "福建",},
					{id: "CN-12", name: "天津",},
					{id: "CN-13", name: "河北",},
					{id: "CN-11", name: "北京",},
					{id: "CN-34", name: "安徽",},
					{id: "CN-36", name: "江西",},
					{id: "CN-37", name: "山东",},
					{id: "CN-41", name: "河南",},
					{id: "CN-43", name: "湖南",},
					{id: "CN-42", name: "湖北",},
					{id: "CN-45", name: "广西",},
					{id: "CN-44", name: "广东",},
					{id: "CN-46", name: "海南",},
					{id: "CN-65", name: "新疆",},
					{id: "CN-64", name: "宁夏",},
					{id: "CN-63", name: "青海",},
					{id: "CN-62", name: "甘肃",},
					{id: "CN-61", name: "陕西",},
					{id: "CN-23", name: "黑龙江",},
					{id: "CN-22", name: "吉林",},
					{id: "CN-21", name: "辽宁",},
				];
				
				
				var mapData = {
					"CN-32": workerslen_jiangsu,
					"CN-52": 0,
					"CN-53": 0,
					"CN-50": 0,
					"CN-51": 0,
					"CN-31": 0,
					"CN-54": 0,
					"CN-33": workerslen_zhejiang,
					"CN-15": 0,
					"CN-14": 0,
					"CN-"  : 0,
					"CN-12": 0,
					"CN-13": 0,
					"CN-11": 0,
					"CN-34": 0,
					"CN-36": 0,
					"CN-37": 0,
					"CN-41": 0,
					"CN-43": 0,
					"CN-42": 0,
					"CN-45": 0,
					"CN-44": 0,
					"CN-46": 0,
					"CN-65": 0,
					"CN-64": 0,
					"CN-63": 0,
					"CN-62": 0,
					"CN-61": 0,
					"CN-23": 0,
					"CN-22": 0,
					"CN-21": 0,
				};

				$('#world-map').vectorMap({
					map: 'cn_mill',
					
					backgroundColor: "transparent",
					regionStyle: {
						initial: {
							fill: '#00c292',
							"fill-opacity": 0.6,
							stroke: 'none',
							"stroke-width": 0,
							"stroke-opacity": 0
						}
					},

					series: {
						regions: [{
							values: mapData,
							scale: ["#cccccc", "#00c292"],
							normalizeFunction: 'polynomial'
						}]
					},
					
					focusOn: {
					  x: 0.5,
					  y: 0.5,
					  scale: 1,
					  animate: true
					},

			　　　　onRegionTipShow: function (event, label, code) {
			　　　　　　$.each(cityName, function (i, items) {
			　　　　　　　　if (code == items.id) {
			　　　　　　　　　　label.html(items.name + " " + mapData[code]);
			　　　　　　　　}
			　　　　　　});
			　　　　},
					
				});
				//end map
				
		});  

		
		
		
  


})(jQuery); 