import sys
import json
import os
import subprocess
import shutil



if (len(sys.argv) == 1):
    print("uso: python publish.py <lambda_diretorio> [profile]")
    sys.exit(1)

profile = 'dev1'

fun = sys.argv[1]
if fun[len(fun)-1] == '/':
    fun = fun[:-1]

if len(sys.argv) > 2:
    profile = sys.argv[2]

if not os.path.isdir(sys.argv[1]):
    print(sys.argv[1]+' nao encontrado')
    sys.exit(1)

cmd = ["aws", "--profile", profile, "lambda" ,"list-functions"]

p = subprocess.Popen(cmd,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

sout = ''
for line in iter(p.stdout.readline, b''):
    sout = sout + line.decode('utf-8')


jls = json.loads(sout)

arn = ''

for f in jls['Functions']:
    name = f['FunctionName']
    if name == fun:
        arn = f['FunctionArn']
        break

if arn == '':
    print("Funcao lambda "+fun+' nao econtrada')
    sys.exit(1)

print('Funcao encontrada '+arn)
zname = "tmp/"+fun+".zip"
try:
    os.remove(zname)
except:
    pass
print('Criando zip em '+zname)
shutil.make_archive(zname, 'zip', fun)

sys.exit(1)

'''

echo "funcao encontrada. arn $arn"


fname="tmp/$1.zip"
echo "criando zip"
rm -rf $fname
#cp sn_http.py python/lib/python3.12/site-packages/
zip -rq $fname $1
echo "zip criado em $fname"



'''
