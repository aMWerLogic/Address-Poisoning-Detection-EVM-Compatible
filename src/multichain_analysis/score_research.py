#used for presenting the values of score similarity function for different log bases
import math
import csv

def calc_score(x, b=1.5):
    c = 8 #2.07
    return math.floor(math.log(x + c, b)) #- 5
    
    
def calc_score1(x):
    if x == 3:
        return 1
    b = 1.5
    c = 2
    return math.floor(math.log(x + c, b)) - 3 
    
    
bs = [1.3, 1.5, 1.7, 1.9, 2, 2.5, 3, 5, 10]

with open("score_research.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["x"] + bs)
    for x in range(1, 34):
        writer.writerow([x] + [calc_score(x, b) for b in bs])