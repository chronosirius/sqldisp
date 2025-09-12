from flask import Blueprint, session, redirect, url_for, request, render_template
from config import GRAPH_CONFIG
from functions import get_db_connection
import networkx as nx
import json

graph = Blueprint("graph", __name__)

@graph.route("/graph")
def graph_route():
    connection = None

    if 'db_user' not in session:
        return redirect(url_for('base_routes.login'))

    try:
        connection = get_db_connection(session['db_user'], session['db_password'])
    except:
        raise
    
    G = nx.Graph()

    weightfactor = request.args.get("weightfactor",3, type=float)
    superign = request.args.get('superignore', type=lambda x: x.split(','))
    min_closeness = request.args.get('min', 0, type=float)
    if superign:
        superign_clause = ""
        for idi in superign:
            superign_clause += f' where p1 != {idi} and p2 != {idi}'
    else:
        superign_clause = ""
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"select {GRAPH_CONFIG["columns"].join(',')} from {GRAPH_CONFIG['table']} inner join {GRAPH_CONFIG['foreign_table']} as t1 on {GRAPH_CONFIG['id1']} = t1.id inner join {GRAPH_CONFIG['foreign_table']} as t2 on {GRAPH_CONFIG['id2']} = t2.id where closeness >= {min_closeness}" + superign_clause +';')
            data = cursor.fetchall()
            for row in data:
                if not G.has_node((nname1 := GRAPH_CONFIG["node_id_generator_j1"](row))):
                    G.add_node(nname1)
                if not G.has_node((nname2 := GRAPH_CONFIG["node_id_generator_j2"](row))):
                    G.add_node(nname2)
                G.add_edge(nname1, nname2, weight=weightfactor/float(row[GRAPH_CONFIG["weights"]]))
    finally:
        connection.close()

    for n in G:
        G.nodes[n]["name"] = n
    
    src = request.args.get("src")
    distance = request.args.get("dist", type=int)
    target = request.args.get("target")
    ignore = request.args.get("ignore", type=lambda x: x.split(','))

    if (src is not None and distance is not None): #type: ignore
        print("YAY SOMEONE KNOWS HOW TO USE TS")
        G = nx.ego_graph(G, src, distance)
    elif (src is not None and target is not None):

        H = nx.Graph()
        for path in list(nx.all_simple_paths(G, src, target)):
            print(path)
            H.add_nodes_from(path)
            print('added nodes')
            H.add_edges_from(zip(path, path[1:]))
            print('added edges')
        G = H

    if ignore is not None:
        for n in ignore:
            try:
                G.remove_node(n)
            except:
                pass

    d = nx.cytoscape_data(G)
    return render_template("graph.html", 
        data = json.dumps(d),
        nodecount = G.number_of_nodes(),
        src = src,
        dist = distance,
        ignore = ignore,
        weightfactor = weightfactor
    )