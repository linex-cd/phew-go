(function ($) {
 "use strict";
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
				{id: "CN-42", name: "河北",},
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
				"CN-32":23,
				"CN-52":23,
				"CN-53":76,
				"CN-50":24,
				"CN-51":45,
				"CN-31":54,
				"CN-54":86,
				"CN-33":53,
				"CN-15":44,
				"CN-14":32,
				"CN-"  :80,
				"CN-12":2,
				"CN-13":56,
				"CN-11":66,
				"CN-34":32,
				"CN-36":12,
				"CN-37":76,
				"CN-41":1,
				"CN-43":35,
				"CN-42":20,
				"CN-45":54,
				"CN-44":29,
				"CN-46":33,
				"CN-65":46,
				"CN-64":62,
				"CN-63":24,
				"CN-62":12,
				"CN-61":57,
				"CN-23":33,
				"CN-22":22,
				"CN-21":14,
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
                        scale: ["#ccccc", "#00c292"],
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
  
})(jQuery); 