import gzip
import os
import sys
import re
import uuid

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime



line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(input_dir,table_name):
    session.execute('TRUNCATE ' + table_name)
    batch_size = 180
    batch_count = 0
    insertion = session.prepare("INSERT INTO " + table_name + " (host,id,datetime,path,bytes) VALUES (?,?,?,?,?)")
    batch = BatchStatement()

    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                if line_re.match(line):
                    batch_count+=1
                    log = line_re.split(line)
                    host = log[1]
                    id_ = uuid.uuid4()
                    dateTime= datetime.strptime(log[2],"%d/%b/%Y:%H:%M:%S")
                    path= log[3]
                    byte = int(log[4])
                    batch.add(insertion, (host,id_,dateTime,path,byte))
                    if batch_count==batch_size:
                        session.execute(batch)
                        batch.clear()
                        batch_count=0
    session.execute(batch)
    batch.clear()


if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]

    cluster = Cluster(['199.60.17.103', '199.60.17.105'])
    session = cluster.connect(keyspace)
    main(input_dir,table_name)

