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
            with open('qwq.txt', 'a') as f:
              f.write(str(ans) + '\n')
        return ans

    workers = min(20 , len(inv))
    with concurrent.futures.ThreadPoolExecutor(workers) as executor:
        results = executor.map(calculate_vertex, inv)
        for result in results:
            ans += result

    print(ans)

# 示例调用
input_file_name1 = 'com-lj.ungraph.txt'
input_file_name2 = 'test-lj-60.txt'
output_file_name = 'result.txt'
tot_ver, upper_ver = count_ver(input_file_name2)
output_file_name2 = "upper_60_lj.txt"
with open(output_file_name2, 'w') as f:
   for item in upper_ver : f.write(str(item) + '\n')

upper_ver = set()
# 从文件中读取数据
with open('upper_60_lj.txt', 'r') as f:
    lines = f.readlines()
    for line in lines:
        item = str(line.strip())
        upper_ver.add(item)
print(len(upper_ver))

indu, outdu = upper_partitions(input_file_name2, upper_ver)
# print(len(indu),len(outdu))
'''
output_file_name3 = "indu_30_lj.txt"
# with open(output_file_name3, 'w') as f:
    # for item in indu : f.write(str(item) + '\n')
indu = set()
with open(output_file_name3, 'r') as f:
    lines = f.readlines()
    for line in lines:
        item = str(line.strip())
        indu.add(item)
print(len(indu))
output_file_name3 = "outdu_30_lj.txt"
outdu = set()
with open(output_file_name3, 'r') as f:
    lines = f.readlines()
    for line in lines:
        item = str(line.strip())
        outdu.add(item)
print(len(outdu))
# with open(output_file_name3, 'w') as f:
    # for item in outdu : f.write(str(item) + '\n')
# print("func2 done")
'''
in_dict, out_dict, in_v = cal_dic(input_file_name2, upper_ver, indu, outdu)

print("over dict : ", len(in_dict), len(out_dict))

'''
output_file_name4 = "inv_30_lj.txt"
in_v = set()
with open(output_file_name4, 'r') as f:
    lines = f.readlines()
    for line in lines:
        item = str(line.strip())
        in_v.add(item)
print(len(in_v))
'''

cal_ans(input_file_name2, in_dict, out_dict, in_v)

'''
print("节点  入度  出度")
for node in in_degrees.keys():
    in_degree = in_degrees[node] if node in in_degrees else 0
    out_degree = out_degrees[node] if node in out_degrees else 0
    print(f"{node}     {in_degree}     {out_degree}")
'''
