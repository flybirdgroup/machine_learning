# coding=utf-8
from flask import Flask, jsonify, render_template
from py2neo import Graph
from build_graph_person import MedicalGraph

app = Flask(__name__)
handler = MedicalGraph()
nodes = handler.get_graph_object().evaluate('MATCH (n) RETURN n')
edges = handler.get_graph_object().evaluate('MATCH (s)-[r]->(e) RETURN e,s,r')
print(nodes.identity)
print(nodes.labels)
print(nodes.__dict__)

print("edges")
print(edges.__dict__)


def buildNodes(nodeRecord):
    data = {"id": str(nodeRecord.identity), "label": next(iter(nodeRecord.labels))}
    # data.update(nodeRecord.n.properties)

    return {"data": data}
# nodes = handler.get_graph_object().evaluate('MATCH (n) RETURN n')
# print(list(nodes))

def buildEdges(relationRecord):
    data = {"source": str(relationRecord.r.start_node._id),
            "target": str(relationRecord.r.end_node._id),
            "relationship": relationRecord.r.rel.type}

    return {"data": data}


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/graph')
def get_graph():
    print(type(handler.get_graph_object().run('MATCH (n) RETURN n')))
    # nodes = map(buildNodes, handler.get_graph_object().evaluate('MATCH (n) RETURN n'))
    nodes = handler.get_graph_object().run('MATCH (n) RETURN n').to_data_frame().to_dict()
    edges = handler.get_graph_object().run('MATCH (s)-[r]->(e) RETURN s,r,e').to_data_frame().to_dict()
    # end_nodes = map(buildNodes, handler.get_graph_object().evaluate('MATCH ()-[r]->(e) RETURN e'))

    # return jsonify(elements={"nodes": nodes})
    return jsonify(elements = {"nodes": nodes, "edges": edges})
#
if __name__ == '__main__':
    app.run(debug = True)