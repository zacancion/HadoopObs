from flask import Flask
import requests
from prometheus_client import generate_latest, REGISTRY, Gauge, Counter
from prometheus_client import CollectorRegistry
import re
import os


app = Flask(__name__)

# 
yarn_api_url = os.environ.get('YARN_API_URL', 'http://default-url-if-not-set')

# Registros separados
cluster_metrics_registry = CollectorRegistry()
app_metrics_registry = CollectorRegistry()

# Métricas del clúster YARN
appsSubmitted = Gauge('yarn_cluster_metrics_appsSubmitted', 'Número total de aplicaciones enviadas', registry=cluster_metrics_registry)
appsCompleted = Gauge('yarn_cluster_metrics_appsCompleted', 'Número total de aplicaciones completadas', registry=cluster_metrics_registry)
appsPending = Gauge('yarn_cluster_metrics_appsPending', 'Número total de aplicaciones pendientes', registry=cluster_metrics_registry)
appsRunning = Gauge('yarn_cluster_metrics_appsRunning', 'Número total de aplicaciones en ejecución', registry=cluster_metrics_registry)
appsFailed = Gauge('yarn_cluster_metrics_appsFailed', 'Número total de aplicaciones fallidas', registry=cluster_metrics_registry)
appsKilled = Gauge('yarn_cluster_metrics_appsKilled', 'Número total de aplicaciones terminadas', registry=cluster_metrics_registry)
reservedMB = Gauge('yarn_cluster_metrics_reservedMB', 'Memoria reservada en MB', registry=cluster_metrics_registry)
availableMB = Gauge('yarn_cluster_metrics_availableMB', 'Memoria disponible en MB', registry=cluster_metrics_registry)
allocatedMB = Gauge('yarn_cluster_metrics_allocatedMB', 'Memoria asignada en MB', registry=cluster_metrics_registry)
reservedVirtualCores = Gauge('yarn_cluster_metrics_reservedVirtualCores', 'Núcleos virtuales reservados', registry=cluster_metrics_registry)
availableVirtualCores = Gauge('yarn_cluster_metrics_availableVirtualCores', 'Núcleos virtuales disponibles', registry=cluster_metrics_registry)
allocatedVirtualCores = Gauge('yarn_cluster_metrics_allocatedVirtualCores', 'Núcleos virtuales asignados', registry=cluster_metrics_registry)
containersAllocated = Gauge('yarn_cluster_metrics_containersAllocated', 'Contenedores asignados', registry=cluster_metrics_registry)
containersReserved = Gauge('yarn_cluster_metrics_containersReserved', 'Contenedores reservados', registry=cluster_metrics_registry)
containersPending = Gauge('yarn_cluster_metrics_containersPending', 'Contenedores pendientes', registry=cluster_metrics_registry)
totalMB = Gauge('yarn_cluster_metrics_totalMB', 'Memoria total en MB', registry=cluster_metrics_registry)
totalVirtualCores = Gauge('yarn_cluster_metrics_totalVirtualCores', 'Total de núcleos virtuales', registry=cluster_metrics_registry)
totalNodes = Gauge('yarn_cluster_metrics_totalNodes', 'Número total de nodos', registry=cluster_metrics_registry)
lostNodes = Gauge('yarn_cluster_metrics_lostNodes', 'Número total de nodos perdidos', registry=cluster_metrics_registry)
unhealthyNodes = Gauge('yarn_cluster_metrics_unhealthyNodes', 'Número total de nodos no saludables', registry=cluster_metrics_registry)
decommissioningNodes = Gauge('yarn_cluster_metrics_decommissioningNodes', 'Nodos en proceso de desmantelamiento', registry=cluster_metrics_registry)
decommissionedNodes = Gauge('yarn_cluster_metrics_decommissionedNodes', 'Nodos desmantelados', registry=cluster_metrics_registry)
rebootedNodes = Gauge('yarn_cluster_metrics_rebootedNodes', 'Nodos reiniciados', registry=cluster_metrics_registry)
activeNodes = Gauge('yarn_cluster_metrics_activeNodes', 'Nodos activos', registry=cluster_metrics_registry)
shutdownNodes = Gauge('yarn_cluster_metrics_shutdownNodes', 'Nodos apagados', registry=cluster_metrics_registry)

# Métricas de aplicaciones YARN en ejecución
allocated_memory_gauge = Gauge('yarn_app_allocated_memory_mb', 'Memoria asignada en MB por aplicación', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
allocated_vcores_gauge = Gauge('yarn_app_allocated_vcores', 'VCores asignados por aplicación', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
memory_seconds_counter = Counter('yarn_app_memory_seconds', 'Segundos de memoria acumulados por aplicación', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
vcore_seconds_counter = Counter('yarn_app_vcore_seconds', 'Segundos de vcore acumulados por aplicación', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
# application_type_gauge = Gauge('yarn_app_application_type', 'Tipo de aplicación', ['app_id'], registry=app_metrics_registry)
elapsed_time_gauge = Gauge('yarn_app_elapsed_time', 'Tiempo transcurrido en milisegundos', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
running_containers_gauge = Gauge('yarn_app_running_containers', 'Contenedores en ejecución', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
reserved_mb_gauge = Gauge('yarn_app_reserved_mb', 'Memoria reservada en MB', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
reserved_vcores_gauge = Gauge('yarn_app_reserved_vcores', 'VCores reservados', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
cluster_usage_percentage_gauge = Gauge('yarn_app_cluster_usage_percentage', 'Porcentaje de uso del clúster', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
preempted_resource_mb_gauge = Gauge('yarn_app_preempted_resource_mb', 'Recursos de memoria preasignados en MB', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
preempted_resource_vcores_gauge = Gauge('yarn_app_preempted_resource_vcores', 'VCores preasignados', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
pending_memory_gauge = Gauge('yarn_app_pending_memory_mb', 'Memoria pendiente', ["app_id","app_type","split_app_name","zone","db_name","table_name"], registry=app_metrics_registry)
sum_pending_memory = Gauge('yarn_app_sum_pending_memory_mb', 'Suma memoria pendiente de todos los app running', registry=app_metrics_registry)


@app.route('/metrics')
def metrics_cluster_endpoint():
        try:
            response = requests.get(f"{yarn_api_url}/ws/v1/cluster/metrics")
            data = response.json()

            # Actualizar métricas del clúster usando los datos obtenidos
            appsSubmitted.set(data['clusterMetrics']['appsSubmitted'])
            appsCompleted.set(data['clusterMetrics']['appsCompleted'])
            appsPending.set(data['clusterMetrics']['appsPending'])
            appsRunning.set(data['clusterMetrics']['appsRunning'])
            appsFailed.set(data['clusterMetrics']['appsFailed'])
            appsKilled.set(data['clusterMetrics']['appsKilled'])
            reservedMB.set(data['clusterMetrics']['reservedMB'])
            availableMB.set(data['clusterMetrics']['availableMB'])
            allocatedMB.set(data['clusterMetrics']['allocatedMB'])
            reservedVirtualCores.set(data['clusterMetrics']['reservedVirtualCores'])
            availableVirtualCores.set(data['clusterMetrics']['availableVirtualCores'])
            allocatedVirtualCores.set(data['clusterMetrics']['allocatedVirtualCores'])
            containersAllocated.set(data['clusterMetrics']['containersAllocated'])
            containersReserved.set(data['clusterMetrics']['containersReserved'])
            containersPending.set(data['clusterMetrics']['containersPending'])
            totalMB.set(data['clusterMetrics']['totalMB'])
            totalVirtualCores.set(data['clusterMetrics']['totalVirtualCores'])
            totalNodes.set(data['clusterMetrics']['totalNodes'])
            lostNodes.set(data['clusterMetrics']['lostNodes'])
            unhealthyNodes.set(data['clusterMetrics']['unhealthyNodes'])
            decommissioningNodes.set(data['clusterMetrics']['decommissioningNodes'])
            decommissionedNodes.set(data['clusterMetrics']['decommissionedNodes'])
            rebootedNodes.set(data['clusterMetrics']['rebootedNodes'])
            activeNodes.set(data['clusterMetrics']['activeNodes'])
            shutdownNodes.set(data['clusterMetrics']['shutdownNodes'])

        except requests.exceptions.RequestException as e:
            print(f"Error al realizar la solicitud: {e}")
            return str(e), 500

        return generate_latest(cluster_metrics_registry)

@app.route('/metrics_yarn_apps')
def metrics_yarn_apps_endpoint():
    try:
        # Limpiar todas las métricas existentes
        for metric in [allocated_memory_gauge, allocated_vcores_gauge, memory_seconds_counter, vcore_seconds_counter, elapsed_time_gauge, running_containers_gauge, reserved_mb_gauge, reserved_vcores_gauge, cluster_usage_percentage_gauge, preempted_resource_mb_gauge, preempted_resource_vcores_gauge]:
            metric.clear()

        # Hacer la solicitud al API y procesar la respuesta
        response = requests.get(f"{yarn_api_url}/ws/v1/cluster/apps/?states=running")
        data_apps = response.json()

        if 'apps' in data_apps and 'app' in data_apps['apps']:
            cluster_pending_memory=0
            for app in data_apps['apps']['app']:
                # Setear labels
                db_name = 'None'
                zone = 'None'
                app_pending_memory=0
                app_id = app['id']
                app_type = app['applicationType']
                split_app_name = app['name'].split(' ', 1)[0]
                ## Labesl de zona-db-table
                patron_table = r"Table \((.*?)\)"
                res = re.search(patron_table, app['name'])
                
                if res is None:
                    table_name = 'None'
                else :
                    table_name = res.group(1).split('.')[1]
                    db_name = res.group(1).split('.')[0]
                    zone = db_name.split('_')[-1]

                # Calcular la suma total de memory pending de todos los container para cada app
                app_pending_memory = sum(partition['pending']['memory'] for partition in app["resourceInfo"]["resourceUsagesByPartition"])
                # Suma total de la memory pendiente para el cluster (todos los app running)
                cluster_pending_memory += app_pending_memory

                
                allocated_memory_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('allocatedMB', 0))
                allocated_vcores_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('allocatedVCores', 0))
                memory_seconds_counter.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).inc(app.get('memorySeconds', 0))
                vcore_seconds_counter.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).inc(app.get('vcoreSeconds', 0))
                # application_type_gauge.labels(app_id).set(app.get('applicationType', ''))
                elapsed_time_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('elapsedTime', 0))
                running_containers_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('runningContainers', 0))
                reserved_mb_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('reservedMB', 0))
                reserved_vcores_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('reservedVCores', 0))
                cluster_usage_percentage_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('clusterUsagePercentage', 0))
                preempted_resource_mb_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('preemptedResourceMB', 0))
                preempted_resource_vcores_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app.get('preemptedResourceVCores', 0))
                pending_memory_gauge.labels(app_id=app_id, app_type=app_type, split_app_name=split_app_name, zone=zone, db_name=db_name, table_name=table_name).set(app_pending_memory)
            
            sum_pending_memory.set(cluster_pending_memory)



    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")
        return str(e), 500

    return generate_latest(app_metrics_registry)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9115)