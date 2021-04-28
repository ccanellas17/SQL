from pyspark import SparkConf, SparkContext
import operator

conf = SparkConf().setMaster("local").setAppName("VideogamesStatistics")
sc = SparkContext(conf = conf)

file = sc.textFile("file:///Users/carlos/PycharmProjects/NONSQL/Project SQL/Games.csv")

#TOP GAME PUBLISHERS (BASED ON HOW MANY GAMES THEY MADE)
def publisher(lines):
    fields=lines.split(";")

    return(fields[4])

publishers=file.map(publisher)
topPublishers=publishers.map(lambda x: (x,1)).reduceByKey(operator.add)
superTopPublishers=topPublishers.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

#TOP GENRES (BASED ON GLOBAL SALES)
def genre(lines):
    fields=lines.split(";")

    return(str(fields[3]), float(fields[9]))

genres=file.map(genre).reduceByKey(lambda x,y: x+y)
TopGenres=genres.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

#TOP GAMES MOST SOLD (BASED ON GLOBAL SALES)
def gameSales(lines):
    fields=lines.split(";")

    return (str(fields[0]), float(fields[9]))

sales = file.map(gameSales)
topSales = sales.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

#YEARS WHERE MOST GAMES WERE SOLD (BASED ON HOW MANY TIMES DOES THE YEAR REPEAT)
def year(lines):
    fields=lines.split(";")

    return(fields[2])

years=file.map(year)
gamesPerYear=years.map(lambda x: (x,1)).reduceByKey(operator.add)
topGamesPerYear=gamesPerYear.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

#HIGHEST SCORES BASED ON CRITICS
def highestCriticScore(lines):
    fields=lines.split(";")

    return(str(fields[0]), fields[10])

criticScoresHigh=file.map(highestCriticScore)
topCriticScores = criticScoresHigh.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

#LOWEST SCORES BASED ON CRITICS
def lowestCriticScore(lines):
    fields=lines.split(";")

    return(str(fields[0]), fields[10])

criticScoresLow=file.map(lowestCriticScore)
lowCriticScores = criticScoresLow.map(lambda x: (x[1], x[0])).sortByKey(ascending=True).collect()

#HIGHEST SCORES BASED ON USERS
def highestUserScore(lines):
    fields=lines.split(";")

    return(str(fields[0]), fields[12])

userScoresHigh=file.map(highestUserScore)
topUsersScores = userScoresHigh.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

#LOWEST SCORES BASED ON USERS
def lowestUserScore(lines):
    fields=lines.split(";")

    return(str(fields[0]), fields[12])

userScoresLow=file.map(lowestUserScore)
lowUsersScores = userScoresLow.map(lambda x: (x[1], x[0])).sortByKey(ascending=True).collect()

a=1
print("TOP GAME PUBLISHERS (BASED ON HOW MANY GAMES THEY MADE):")
for results in superTopPublishers[:10]:
    print("   ", a,")",results[1], ":", results[0], "games.")

    a=a+1

print("")

b=1
print("TOP GENRES (BASED ON GLOBAL SALES):")
for results in TopGenres[:10]:
    print("   ", b,")",results[1], ":", "{:.2f}".format(results[0]), "million units.")
    b=b+1

print("")

c=1
print("TOP GAMES MOST SOLD (BASED ON GLOBAL SALES):")
for results in topSales[:10]:
    print("   ", c,")", results[1], ":", results[0], "million units")
    c=c+1

print("")

d=1
print("YEARS WHERE MORE GAMES WERE SOLD (BASED ON HOW MANY TIMES DOES THE YEAR REPEAT):")
print("   Top 10 years where more games were sold:")
for results in topGamesPerYear[:10]:
    print("     ", d,")",results[1], ":", results[0], "games")
    d=d+1

print("")
e=1
print("   Top 10 years where less games were sold:")
for results in topGamesPerYear[-16:-6]:
    print("     ", e,")",results[1], ":", results[0], "games")
    e=e+1

print("")

print("HIGHEST SCORES:")
f=1
print("   HIGHEST SCORES BASED ON CRITIC SCORE:")
for results in topCriticScores[:10]:
    print("      ", f,")", results[1], ":", results[0], "score")
    f=f+1

print("")

count=0
print("   HIGHEST SCORES BASED ON USER SCORE:")
for results in topUsersScores:
    if results[0]!="tbd":
        print("      ", count+1,")", results[1], ":", int(float(results[0])*10), "score")
        count = count + 1
        if count==10:
             break

print("")

print("LOWEST SCORES:")
h=0
print("   LOWEST SCORES BASED ON CRITIC SCORE:")
for results in lowCriticScores:
    if results[0]!="":
        if int(float(results[0]))>=13:
            print("      ", h+1,")", results[1], ":", results[0], "score")
            h = h + 1
            if h==10:
                break

print("")

count2=0
print("   LOWEST SCORES BASED ON USER SCORE:")
for results in lowUsersScores:
    if results[0]!="":
        print("      ", count2+1,")", results[1], ":", int(float(results[0])*10), "score")
        count2 = count2 + 1
        if count2==10:
             break
