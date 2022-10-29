#!/usr/bin/env python3
# coding: utf-8
from py2neo import Graph, Node, Relationship
import pandas as pd
import re
import os


class MedicalGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path = os.path.join(cur_dir, 'DATA/new_joiner.csv')
        self.graph = Graph("bolt://localhost:7687", auth=("neo4j", "123456"))

    def read_file(self):
        """
        读取文件，获得实体，实体关系
        :return:
        """
        # cols = ["name", "alias", "hobby", "age", "infection", "insurance", "department", "checklist", "education",
        #         "complication", "treatment", "drug", "period", "rate", "money"]
        # 实体
        new_joiners = []  # 新员工
        locations = []  # 别名
        educations = []  # 症状
        hobbies = []  # 部位
        departments = []  # 科室
        skillsets = []  # 并发症


        # 疾病的属性：age, infection, insurance, checklist, treatment, period, rate, money
        new_joiners_infos = []
        # 关系
        new_joiners_to_education = []  # 疾病与症状关系
        new_joiners_to_location = []  # 疾病与别名关系
        new_joiners_to_hobby = []  # 疾病与部位关系
        new_joiners_to_department = []  # 疾病与科室关系
        new_joiners_to_skillset = []  # 疾病与并发症关系


        all_data = pd.read_csv(self.data_path, encoding='utf-8').loc[:, :].values
        for data in all_data:
            new_joiner_dict = {}  # 疾病信息
            # 疾病
            new_joiner = str(data[0]).replace("...", " ").strip()
            new_joiner_dict["name"] = new_joiner
            # 别名
            line = re.sub("[，、；,.;]", " ", str(data[1])) if str(data[1]) else "unknown"
            for loc in line.strip().split():
                locations.append(loc)
                new_joiners_to_location.append([new_joiner, loc])

            education_list = str(data[2]).replace("...", " ").strip().split()[:-1]
            for education in education_list:
                educations.append(education)
                new_joiners_to_education.append([new_joiner, education])

            # 部位
            hobby_list = str(data[3]).strip().split() if str(data[3]) else "unknown"
            for hobby in hobby_list:
                hobbies.append(hobby)
                new_joiners_to_hobby.append([new_joiner, hobby])

            # 并发症
            skillset_list = str(data[4]).strip().split()[:-1] if str(data[4]) else "unknown"
            for skillset in skillset_list:
                skillsets.append(skillset)
                new_joiners_to_skillset.append([new_joiner, skillset])
 
            # 科室
            department_list = str(data[5]).strip().split()
            for department in department_list:
                departments.append(department)
                new_joiners_to_department.append([new_joiner, department])

            # 年龄
            age = str(data[6]).strip()
            new_joiner_dict["age"] = age

            gender = str(data[7]).strip() if str(data[7]) else "unknown"
            new_joiner_dict["gender"] = gender



            new_joiners_infos.append(new_joiner_dict)

        return set(new_joiners), set(educations), set(locations), set(hobbies), set(departments), set(skillsets), \
                new_joiners_to_location, new_joiners_to_education, new_joiners_to_hobby, new_joiners_to_department, \
                new_joiners_to_skillset, new_joiners_infos

    def create_node(self, label, nodes):
        """
        创建节点
        :param label: 标签
        :param nodes: 节点
        :return:
        """
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name, type=label)
            self.graph.create(node)
            count += 1
            print(count, len(nodes))
        return

    def create_new_joiners_nodes(self, new_joiner_info):
        """
        创建疾病节点的属性
        :param new_joiner_info: list(Dict)
        :return:
        """
        count = 0
        for new_joiner_dict in new_joiner_info:
            node = Node("new_joiner", name=new_joiner_dict['name'], age=new_joiner_dict['age'],
                        gender=new_joiner_dict['gender'],type="new_joiner")
            self.graph.create(node)
            count += 1
            print(count)
        return

    def create_graphNodes(self):
        """
        创建知识图谱实体
        :return:
        """
        new_joiner, education, loc, hobby, department, skillset, rel_loc, rel_education, rel_hobby, \
        rel_department, rel_skillset, rel_infos = self.read_file()
        self.create_new_joiners_nodes(rel_infos)
        self.create_node("education", education)
        self.create_node("loc", loc)
        self.create_node("hobby", hobby)
        self.create_node("Department", department)
        self.create_node("skillset", skillset)

        return

    def create_graphRels(self):
        new_joiner, education, loc, hobby, department, skillset, rel_loc, rel_education, rel_hobby, \
        rel_department, rel_skillset, rel_infos = self.read_file()

        self.create_relationship("new_joiner", "loc", rel_loc, "Location_IS", "location")
        self.create_relationship("new_joiner", "education", rel_education, "HAS_studied", "education")
        self.create_relationship("new_joiner", "hobby", rel_hobby, "Hobby_IS", "hobby")
        self.create_relationship("new_joiner", "skillset", rel_skillset, "HAS_skillset", "skill")
        self.create_relationship("new_joiner", "Department", rel_department, "DEPARTMENT_IS", "department")

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        """
        创建实体关系边
        :param start_node:
        :param end_node:
        :param edges:
        :param rel_type:
        :param rel_name:
        :return:
        """
        count = 0
        # 去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.graph.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
        return

    def get_graph_object(self):
        return self.graph



if __name__ == "__main__":
    handler = MedicalGraph()
    handler.create_graphNodes()
    handler.create_graphRels()
