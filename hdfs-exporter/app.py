from flask import Flask
import requests
from prometheus_client import generate_latest, REGISTRY, Gauge, Counter
from prometheus_client import CollectorRegistry
import re
import os


app = Flask(__name__)

# 
api_url = os.environ.get('HDFS_API_URL', 'http://default-url-if-not-set')
api_endpoint = "/jmx"

# Registros separados
metrics_registry = CollectorRegistry()

# Métricas del clúster YARN
capacitytotal = Gauge('hdfs_metrics_CapacityTotal', 'CapacityTotal en Bytes', registry=metrics_registry)
capacityused = Gauge('hdfs_metrics_CapacityUsed', 'CapacityUsed en Bytes', registry=metrics_registry)
capacityremaining = Gauge('hdfs_metrics_CapacityRemaining', 'CapacityRemaining en Bytes', registry=metrics_registry)
blockstotal = Gauge('hdfs_metrics_BlocksTotal', 'BlocksTotal', registry=metrics_registry)
filestotal = Gauge('hdfs_metrics_FilesTotald', 'FilesTotal', registry=metrics_registry)
numlivedatanodes = Gauge('hdfs_metrics_NumLiveDataNodes', 'NumLiveDataNodes', registry=metrics_registry)
numdeaddatanodes = Gauge('hdfs_metrics_NumDeadDataNodes', 'NumDeadDataNodes', registry=metrics_registry)
numdecomlivedatanodes = Gauge('hdfs_metrics_NumDecomLiveDataNodes', 'NumDecomLiveDataNodes', registry=metrics_registry)
numdecomdeaddatanodes = Gauge('hdfs_metrics_NumDecomDeadDataNodes', 'NumDecomDeadDataNodes', registry=metrics_registry)



@app.route('/metrics')
def metrics_cluster_endpoint():
    try:
        response = requests.get(f"{api_url}{api_endpoint}")
        data = response.json()
        fsname_state_data = next((bean for bean in data["beans"] if bean["name"] == "Hadoop:service=NameNode,name=FSNamesystemState"), None)

        # Actualizar métricas del clúster usando los datos obtenidos
        capacitytotal.set(fsname_state_data['CapacityTotal'])
        capacityused.set(fsname_state_data['CapacityUsed'])
        capacityremaining.set(fsname_state_data['CapacityRemaining'])

        blockstotal.set(fsname_state_data['BlocksTotal'])
        filestotal.set(fsname_state_data['FilesTotal'])
        numlivedatanodes.set(fsname_state_data['NumLiveDataNodes'])
        numdeaddatanodes.set(fsname_state_data['NumDeadDataNodes'])
        numdecomlivedatanodes.set(fsname_state_data['NumDecomLiveDataNodes'])
        numdecomdeaddatanodes.set(fsname_state_data['NumDecomDeadDataNodes'])


    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")
        return str(e), 500

    return generate_latest(metrics_registry)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9116)