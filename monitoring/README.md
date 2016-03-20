docker run \
--interactive \
--tty \
--detach \
--volume /data:/usr/share/elasticsearch/data \
--publish 9200:9200 \
--publish 9300:9300 \
--name monitoring-server-elasticsearch \
--log-driver json-file --log-opt max-size=10m \
elasticsearch


docker run \
--interactive \
--tty \
--detach \
--publish 5601:5601 \
--name monitoring-server-kibana \
--log-driver json-file --log-opt max-size=10m \
--link monitoring-server-elasticsearch:elasticsearch \
kibana



curl --header "kbn-version:4.4.2" -XPOST "http://localhost:5601/elasticsearch/.kibana/index-pattern/monitoring?op_type=create" -d '
    {
        "title": "monitoring",
        "timeFieldName": "date",
        "customFormats":"{}"
    }
'

curl --header "kbn-version:4.4.2" -XPOST "http://localhost:5601/elasticsearch/.kibana/config/4.4.2/_update" -d '
    {
        "doc": {
            "doc": {
                "defaultIndex": "monitoring"
            },
            "defaultIndex": "monitoring"
        }
    }
'

curl --header "kbn-version:4.4.2" -XPOST "http://127.0.0.1:5601/elasticsearch/.kibana/dashboard/tasks_dashboard?op_type=create" -d '
    {"title":"tasks_dashboard","hits":0,"description":"","panelsJSON":"[{\"col\":1,\"id\":\"total_success_counter\",\"panelIndex\":1,\"row\":1,\"size_x\":4,\"size_y\":2,\"type\":\"visualization\"},{\"col\":9,\"id\":\"total_retry_counter\",\"panelIndex\":2,\"row\":1,\"size_x\":4,\"size_y\":2,\"type\":\"visualization\"},{\"col\":5,\"id\":\"total_failure_counter\",\"panelIndex\":3,\"row\":1,\"size_x\":4,\"size_y\":2,\"type\":\"visualization\"},{\"col\":1,\"id\":\"tasks_per_minute_bar\",\"panelIndex\":4,\"row\":3,\"size_x\":12,\"size_y\":5,\"type\":\"visualization\"}]","optionsJSON":"{\"darkTheme\":false}","uiStateJSON":"{}","version":1,"timeRestore":false,"kibanaSavedObjectMeta":{"searchSourceJSON":"{\"filter\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}}}]}"}}
'

curl --header "kbn-version:4.4.2" -XPOST "http://127.0.0.1:5601/elasticsearch/.kibana/visualization/total_success_counter?op_type=create" -d '
    {"title":"total_success_counter","visState":"{\"title\":\"total_success_counter\",\"type\":\"metric\",\"params\":{\"fontSize\":60},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}}],\"listeners\":{}}","uiStateJSON":"{}","description":"","version":1,"kibanaSavedObjectMeta":{"searchSourceJSON":"{\"index\":\"monitoring\",\"query\":{\"query_string\":{\"query\":\"type:1\",\"analyze_wildcard\":true}},\"filter\":[]}"}}
'

curl --header "kbn-version:4.4.2" -XPOST "http://127.0.0.1:5601/elasticsearch/.kibana/visualization/total_failure_counter?op_type=create" -d '
    {"title":"total_failure_counter","visState":"{\"title\":\"total_failure_counter\",\"type\":\"metric\",\"params\":{\"fontSize\":60},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}}],\"listeners\":{}}","uiStateJSON":"{}","description":"","version":1,"kibanaSavedObjectMeta":{"searchSourceJSON":"{\"index\":\"monitoring\",\"query\":{\"query_string\":{\"query\":\"type:2\",\"analyze_wildcard\":true}},\"filter\":[]}"}}
'

curl --header "kbn-version:4.4.2" -XPOST "http://127.0.0.1:5601/elasticsearch/.kibana/visualization/total_retry_counter?op_type=create" -d '
    {"title":"total_retry_counter","visState":"{\"title\":\"total_retry_counter\",\"type\":\"metric\",\"params\":{\"fontSize\":60},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}}],\"listeners\":{}}","uiStateJSON":"{}","description":"","version":1,"kibanaSavedObjectMeta":{"searchSourceJSON":"{\"index\":\"monitoring\",\"query\":{\"query_string\":{\"query\":\"type:3\",\"analyze_wildcard\":true}},\"filter\":[]}"}}
'

curl --header "kbn-version:4.4.2" -XPOST "http://127.0.0.1:5601/elasticsearch/.kibana/visualization/tasks_per_minute_bar?op_type=create" -d '
    {"title":"tasks_per_minute_bar","visState":"{\"title\":\"tasks_per_minute_bar\",\"type\":\"histogram\",\"params\":{\"shareYAxis\":true,\"addTooltip\":true,\"addLegend\":true,\"scale\":\"linear\",\"mode\":\"stacked\",\"times\":[],\"addTimeMarker\":false,\"defaultYExtents\":false,\"setYExtents\":false,\"yAxis\":{}},\"aggs\":[{\"id\":\"1\",\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"type\":\"date_histogram\",\"schema\":\"segment\",\"params\":{\"field\":\"date\",\"interval\":\"m\",\"customInterval\":\"2h\",\"min_doc_count\":1,\"extended_bounds\":{}}}],\"listeners\":{}}","uiStateJSON":"{}","description":"","version":1,"kibanaSavedObjectMeta":{"searchSourceJSON":"{\"index\":\"monitoring\",\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}},\"filter\":[]}"}}
'


http://127.0.0.1:5601/app/kibana#/dashboard/tasks_dashboard?_g=(refreshInterval:(display:'10%20seconds',pause:!f,section:1,value:10000),time:(from:now-24h,mode:quick,to:now))&_a=(filters:!(),options:(darkTheme:!f),panels:!((col:1,id:total_success_counter,panelIndex:1,row:1,size_x:4,size_y:2,type:visualization),(col:9,id:total_retry_counter,panelIndex:2,row:1,size_x:4,size_y:2,type:visualization),(col:5,id:total_failure_counter,panelIndex:3,row:1,size_x:4,size_y:2,type:visualization),(col:1,id:tasks_per_minute_bar,panelIndex:4,row:3,size_x:12,size_y:5,type:visualization)),query:(query_string:(analyze_wildcard:!t,query:'*')),title:tasks_dashboard,uiState:())
