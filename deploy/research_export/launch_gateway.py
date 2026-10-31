import sys
sys.path.insert(0,'/opt/research-export')
from research_export.gateway import main
sys.argv=[sys.argv[0],'--config','/etc/research-export/gateway.json']
main()
