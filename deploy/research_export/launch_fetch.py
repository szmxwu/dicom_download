import sys
sys.path.insert(0,'/opt/research-fetch')
from research_export.fetch import main
sys.argv=[sys.argv[0],'serve','--config','/etc/research-fetch/config.json']
main()
