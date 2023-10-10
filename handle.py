# from collections import Counter
# import itertools

import concurrent.futures

def upper_partitions(file_name, upper_v):
    with open(file_name, 'r') as file:
        lines = file.readlines()
        partition_id = None
        out_degrees = {}
        in_degrees = {}
        edges = []
        v_tot = 0

        # print(in_d,out_d,file="qwq.txt")
        #with open("qwq.txt", "w") as file:
         #   file.write(f"{in_d}")

        for line in lines:
            line = line.strip()
            # print(line)
            # print(partition_id)
            if line:
                partition_id = line.split(':')[0]
                # print(partition_id)
                line = line.split(':')[1]
                edges = line.split(",")
                # print(len(edges),len(line))
              
                if partition_id is not None:
                    
                    ver = set()
                    for edge in edges:
                      if edge:
                      #print(edge,edge.split(' '))
                        source = edge.split(' ')[0]
                        target = edge.split(' ')[-1]
                        ver.add(source)
                        ver.add(target)

                        if((target in upper_v) and (source in upper_v)) : 
                          if target in in_degrees : in_degrees[target] += 1
                          else : in_degrees[target] = 1
                          if source in out_degrees : out_degrees[source] += 1
                          else : out_degrees[source] = 1                              
                    
                    v_tot += len(ver)
        print(" RP:",v_tot)
        return in_degrees, out_degrees

def count_ver(file_name):
    in_degrees = {}
    out_degrees = {}
    with open(file_name, 'r') as file:
        print("read done")
        lines = file.readlines()
        partition_id = None
        edges = []
        tot_ver = set()
        upp_ver = set()

        # print(in_d,out_d,file="qwq.txt")
        #with open("qwq.txt", "w") as file:
         #   file.write(f"{in_d}")

        for line in lines:
            line = line.strip()
            # print(line)
            # print(partition_id)
            if line:
                partition_id = line.split(':')[0]
                line = line.split(':')[1]
                edges = line.split(",")
                vertice = set()
              
                if partition_id is not None:
                    # if partition_id % 100 == 99 : print("p:",partition_id)
                    for edge in edges:
                      if edge:
                      #print(edge,edge.split(' '))
                        source = edge.split(' ')[0]
                        target = edge.split(' ')[-1]
                        if source in tot_ver : upp_ver.add(source)
                        else : vertice.add(source)
                        if target in tot_ver : upp_ver.add(target)
                        else : vertice.add(target)
                tot_ver = tot_ver.union(vertice)
                # print(partition_id, len(tot_ver), len(upp_ver))
    return tot_ver, upp_ver

def cal_dic(file_name, upper_v, indu, outdu):
    with open(file_name, 'r') as file:
        lines = file.readlines()
        partition_id = None
        in_v = {}
        out_v = {}
        inv_all = set()

        # print(in_d,out_d,file="qwq.txt")
        #with open("qwq.txt", "w") as file:
         #   file.write(f"{in_d}")

        for line in lines:
            line = line.strip()
            # print(line)
            # print(partition_id)
            if line:
                partition_id = line.split(':')[0]
                line = line.split(':')[1]
                edges = line.split(",")
              
                if partition_id is not None:
                    
                    in_dp = {}
                    out_dp = {}
                    upper_p = set()
                    in_v[partition_id] = set()
                    out_v[partition_id] = set()
                    for edge in edges:
                      if edge:
                      #print(edge,edge.split(' '))
                        source = edge.split(' ')[0]
                        target = edge.split(' ')[-1]

                        if(source in upper_v) : upper_p.add(source)
                        if(target in upper_v) : upper_p.add(target)
                        if((target in upper_v) and (source in upper_v)) :  
                          if target in in_dp : in_dp[target] += 1
                          else : in_dp[target] = 1
                          if source in out_dp : out_dp[source] += 1
                          else : out_dp[source] = 1
                    for edge in edges:
                      if edge:
                      #print(edge,edge.split(' '))
                        source = edge.split(' ')[0]
                        target = edge.split(' ')[-1]
                        if((target in upper_v) and (target in in_dp)) : 
                          if in_dp[target] != indu[target] : 
                             in_v[partition_id].add(target)
                             inv_all.add(target)
                        if((source in upper_v) and (source in out_dp)) :
                           if out_dp[source] != outdu[source] : out_v[partition_id].add(source)
        return in_v, out_v, inv_all

def cal_ans(file_name, indic, outdic, inv):

    def calculate_vertex(v):
        # nonlocal ans
        with open(file_name, 'r') as file:
            lines = file.readlines()
            partition_id = None
            t = 0
            to = set()
            ans = 0
            for line in lines:
                line = line.strip()
                if line:
                    partition_id = line.split(':')[0]
                    line = line.split(':')[1]

                    if partition_id is not None:
                        if v in indic[partition_id]:
                            to = to.union(outdic[partition_id])
                            t += 1
                            # print(len(to), ans, t)
            ans = len(to)
        return ans

    # workers = min(20 , len(inv))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(calculate_vertex, inv)
        for result in results:
            ans += result

    print(ans)

# 示例调用
input_file_name1 = 'com-lj.ungraph.txt'
input_file_name2 = 'test-lj-60.txt'
output_file_name = 'result.txt'
tot_ver, upper_ver = count_ver(input_file_name2)

indu, outdu = upper_partitions(input_file_name2, upper_ver)

in_dict, out_dict, in_v = cal_dic(input_file_name2, upper_ver, indu, outdu)


cal_ans(input_file_name2, in_dict, out_dict, in_v)
