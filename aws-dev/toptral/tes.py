
def compare(a,b):
    removed = [x for x in a if x not in b]
    added = [x for x in b if x not in a]
    intersect = [x for x in a if x in b]
    return [removed,added,intersect]

def difference(b, a):
    c = list(b)
    for item in a:
       try:
           c.remove(item)
       except ValueError:
           pass            #or maybe you want to keep a values here
    return c

def solution(S, T):
    result = None
    removed, added, intersect = compare(S,T)
    print(removed,added,intersect)
    if difference((list(S)),intersect) ==  None:
        result = "IMPOSSIBLE"
    elif len(intersect) < len(S) and len(added) > 0 and len(removed)> 0:
        one = ''.join(removed)
        two = ''.join(added)
        result = "SWAP " + one + " " + two
    elif len(intersect) < len(T) and len(added) == 0 and len(removed) == 0:
        tlist=list(T)
        diff = difference(tlist,intersect)
        diffstring = ''.join(diff)
        result = "INSERT " + diffstring

    else:
        print("swap/replace")
    print(result)
    return result


def main():
    solution("test","tent")
    solution("nice","niece")
    solution("o", "off")



if __name__ == "__main__":
    main()