from pyspark import SparkContext
from pyspark import SQLContext
from operator import add

sc = SparkContext("local","First_app")

sql_context = SQLContext(sc)

path = "C:/Users/Roche/Documents/Python/crawler_bestiary/bestiary.json"

df = sql_context.read.json(path,multiLine=True)
print(df.dtypes)

print(df)
df.show()

def get_spell(ligne):
    resultat = []
    print(ligne)
    for i in ligne.spells:
        resultat.append([i,ligne.name])
    return resultat

r1 = df.rdd.flatMap(lambda ligne: get_spell(ligne)).reduceByKey(lambda a, b: a+', '+b).collect()
r2 = sql_context.createDataFrame(r1, ['spell','names']).collect()

for i in r2:
    print(i)


