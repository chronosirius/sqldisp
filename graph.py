from flask import Blueprint, session, redirect, url_for, request, render_template
from config import GRAPH_CONFIGS
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
    min_weight = request.args.get('min', 0, type=float)
    only_with_tag_one = request.args.get("only_one")
    only_with_tag_both = request.args.get("only_both")
    ignore_type = request.args.get("ignore_ttype", [], type=lambda x: x.split(',')) or []

    if superign:
        superign_clause = ""
        for idi in superign:
            superign_clause += f' and p1 != {idi} and p2 != {idi}'
    else:
        superign_clause = ""

    try:
        with connection.cursor() as cursor:
            for gconf in GRAPH_CONFIGS:
                if gconf['table'] in ignore_type:
                    continue
                if only_with_tag_one:
                    tag_only_clause_one = f""" \
            and (exists (select 1 from {gconf['tags_jct_table']['name']} where p1 = {gconf['tags_jct_table']['c1']} and {gconf['tags_jct_table']['c2']} in ({only_with_tag_one})) \
            or exists (select 1 from {gconf['tags_jct_table']['name']} where p2 = {gconf['tags_jct_table']['c1']} and {gconf['tags_jct_table']['c2']} in ({only_with_tag_one}))) """
                else:
                    tag_only_clause_one = ""

                if only_with_tag_both:
                    tag_only_clause_both = f""" \
            and (exists (select 1 from {gconf['tags_jct_table']['name']} where p1 = {gconf['tags_jct_table']['c1']} and {gconf['tags_jct_table']['c2']} in ({only_with_tag_both})) \
            and exists (select 1 from {gconf['tags_jct_table']['name']} where p2 = {gconf['tags_jct_table']['c1']} and {gconf['tags_jct_table']['c2']} in ({only_with_tag_both}))) 
            """
                else:
                    tag_only_clause_both = ""

                q = f"select {','.join(gconf["columns"])} from {gconf['table']} inner join {gconf['foreign_table']} as t1 on {gconf['id1']} = t1.id inner join {gconf['foreign_table']} as t2 on {gconf['id2']} = t2.id where {gconf['weights']} >= {min_weight} " + superign_clause + tag_only_clause_one + tag_only_clause_both + (" and ".join([''] + gconf['sqlextras'])) + ' ;'

                print(q)
                cursor.execute(q)
                data = cursor.fetchall()
                for row in data:
                    if not G.has_node((nname1 := gconf["node_id_generator_j1"](row))):
                        G.add_node(nname1)
                    if not G.has_node((nname2 := gconf["node_id_generator_j2"](row))):
                        G.add_node(nname2)
                    G.add_edge(nname1, nname2, weight=weightfactor/float(row[gconf["weights"]]), **gconf['attrs'])
    finally:
        connection.close()

    for n in G:
        G.nodes[n]["name"] = n
    
    src = request.args.get("src")
    distance = request.args.get("dist", type=int)
    target = request.args.get("target")
    shortest_only = request.args.get("shortest_only", False, type=bool)
    no_ignore_weights = request.args.get("no_ignore_weights", False, type=bool)
    ignore = request.args.get("ignore", type=lambda x: x.split(','))
    cutoff = request.args.get("cutoff", 4, type=lambda x:min(int(x), 7))

    if (src is not None and distance is not None): #type: ignore
        print("YAY SOMEONE KNOWS HOW TO USE TS")
        G = nx.ego_graph(G, src, distance)
    elif (src is not None and target is not None):
        print('src not none and target specified')
        H = nx.Graph()
        print('H created')
        if shortest_only:
            paths = nx.all_shortest_paths(G, src, target, 'weight' if no_ignore_weights else None) 
        else:
            paths = nx.all_simple_paths(G, src, target, cutoff)
        for path in paths:
            #print(path)
            H.add_nodes_from(path)
            #print('added nodes')
            H.add_weighted_edges_from(zip(path, path[1:], [G.get_edge_data(path[x], path[x+1])["weight"] for x in range(len(path)-1)]))
            #print('added edges')
        G = H

    if ignore is not None:
        for n in ignore:
            try:
                G.remove_node(n)
            except:
                pass

    for node_id in G.nodes():
        G.nodes[node_id]['cliques'] = []


    d = nx.cytoscape_data(G)
    return render_template("graph.html", 
        data = json.dumps(d),
        nodecount = G.number_of_nodes(),
        src = src,
        dist = distance,
        ignore = ignore,
        weightfactor = weightfactor,
    )