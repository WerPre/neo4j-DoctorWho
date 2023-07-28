import os
import json
import re
import itertools
from datetime import datetime, timedelta
from neo4j import GraphDatabase

data_folder = "data"

def create_planet(tx, planet):
    print(planet)
    tx.run("CREATE (a:Planet {name: $name})", name=planet)


def create_enemy(tx, enemy):
    str_merge = 'MERGE (a:Enemy {name: "' + enemy["name"] + '"})\n'
    str_match = 'MATCH (a:Enemy {name: "' + enemy["name"] + '"})'
    tx.run(str_merge)
    command = str_match + ', (d:Doctor)\nCREATE (a)-[:ENEMY_OF]->(d)'
    tx.run(command)
    for key in enemy.keys():
        # if type(enemy[key]) != list:
        #     command += key + ': ' + "'" + enemy[key] + "'"
        if key == "planet":
            str_node_create_if_need = 'MERGE (p:Planet {name: "' + enemy[key] + '"})'
            command = str_match + str_node_create_if_need+ "MERGE (a) -[:COMES_FROM]->(p)"
            tx.run(command)
        if key == "belonings":
            for b in enemy[key]:
                str_node_create_if_need = 'MERGE (b:Beloning {name: "' + b + '"})\n'
                command = str_match + str_node_create_if_need + 'MERGE (a) -[:HAS]->(b)'
                tx.run(command)
                #tx.run("MATCH (a:Enemy {name: $name})\nMERGE (a) -[:HAS]->(b:Beloning {name: $beloning})", name = enemy["name"], beloning=b)
        elif key == "species":
            for spec in enemy[key]:
                str_node_create_if_need = 'MERGE (s:Species {name: "' + spec + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (a) -[:IS_A]->(s)"
                tx.run(command)
                #tx.run("MATCH (a:Enemy {name: $name})\nMERGE (a) -[:IS_A]->(s:Species {name: $species})", name = enemy["name"], species=spec)
        elif key == "regenerations":
            for actor in enemy[key]:
                str_node_create_if_need = 'MERGE (b:Actor {name: "' + actor["actor/actress"] + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (a) <-[:REGENERATION]-(b)"
                tx.run(command)

                #tx.run("MERGE (a:Enemy {name: $name1})\n MERGE (a) <-[:REGENERATION] - (b:Actor {name: $name2})", name1= enemy["name"], name2 = actor["actor/actress"])
                #tx.run("MATCH (a:Enemy {name: $name1})\nMERGE (a) <-[:REGENERATION] - (b:Actor {name: $name2})", name1= enemy["name"], name2 = actor["actor/actress"])

    #     else:
    #         command += key + ': ' + str(enemy[key])
    #     command += ', '
    # command = command[:-2]

    # print(command)
    # tx.run("MERGE (a:Enemy {" + command + "})")

def create_ally(tx, ally):
    str_merge = 'MERGE (a:Ally {name: "' + ally["name"] + '"})\n'
    str_match = 'MATCH (a:Ally {name: "' + ally["name"] + '"})'
    tx.run(str_merge)
    command = str_match + ',(d:Doctor)\nCREATE (a)-[:ALLY_OF]->(d)'
    tx.run(command)
    for key in ally.keys():
        # if type(ally[key]) != list:
        #     command += key + ': ' + "'" + ally[key] + "'"
        if key == "planet":
            str_node_create_if_need = 'MERGE (p:Planet {name: "' + ally[key] + '"})\n'
            command = str_match + str_node_create_if_need + "MERGE (a) -[:COMES_FROM]->(p)"
            tx.run(command)

            #tx.run("MATCH (a:Ally {name: $name})\nMERGE (a) -[:COMES_FROM]->(p:Planet {name: $planet})", name = ally["name"], planet=ally[key])
        elif key == "actor/actress":
            str_node_create_if_need = 'MERGE (ac:Actor {name: "' + ally[key] + '"})\n'
            command = str_match + str_node_create_if_need + "MERGE (a) -[:PLAYED_BY]->(ac)"
            tx.run(command)
            # tx.run("MATCH (a:Ally {name: $name})\nMERGE (a) -[:PLAYED_BY]->(ac:Actor {name: $actor})", name = ally["name"], actor=ally[key])
        elif key == "belonings":
            for b in ally[key]:
                str_node_create_if_need = 'MERGE (b:Beloning{name: "'+ b +'"})\n'
                command = str_match + str_node_create_if_need + "MERGE (a) -[:HAS]->(b)"
                tx.run(command)
                #tx.run("MATCH (a:Ally {name: $name})\nMERGE (a) -[:HAS]->(b:Beloning {name: $beloning})", name = ally["name"], beloning=b)
        elif key == "childOf":
            for p in ally[key]:
                str_node_create_if_need = 'MERGE (c:Companion{name: "'+ p +'"})\n' #to c jest robocze
                command = str_match + str_node_create_if_need + "MERGE (a) -[:CHILD_OF]->(c)" 
                tx.run(command)
                #tx.run("MATCH (a:Ally {name: $name})\\nMERGE (a) -[:CHILD_OF]->(c:Companion {name: $parent})", name = ally["name"], parent=p) #robocze
        elif key == "species":
            for spec in ally[key]:
                str_node_create_if_need = 'MERGE (s:Species {name: "' + spec + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (a) -[:IS_A]->(s)"
                tx.run(command)
                #.run("MATCH (a:Ally {name: $name})\nMERGE (a) -[:IS_A]->(s:Species {name: $species})", name = ally["name"], species=spec)
        elif key == "firstAppearedIn":
            tx.run("MATCH (a:Ally {name: $name})\nMERGE (a) -[:FIRST_APPEARED_IN]->(ep:Episode {number: $nr, nameOfEpisode : $ep_name})", name = ally["name"], nr=ally[key]["number"], ep_name=ally[key]["nameOfEpisode"])
        elif key == "diedIn":
            tx.run("MATCH (a:Ally {name: $name})\nMERGE (a) -[:DIED_IN]->(ep:Episode {number: $nr, nameOfEpisode : $ep_name})", name = ally["name"], nr=ally[key]["number"], ep_name=ally[key]["nameOfEpisode"])
        elif key == "regenerations":
            for actor in ally[key]:
                #tx.run("MERGE (a:ally {name: $name1})\n MERGE (a) <-[:REGENERATION] - (b:Actor {name: $name2})", name1= ally["name"], name2 = actor["actor/actress"])
                tx.run("MATCH (a:Ally {name: $name1})\nMERGE (a) <-[:REGENERATION] - (b:Actor {name: $name2})", name1= ally["name"], name2 = actor["actor/actress"])

def create_companion(tx, companion):
    str_merge = 'MERGE (a:Companion {name: "' + companion["name"] + '"})\n'
    str_match = 'MATCH (a:Companion {name: "' + companion["name"] + '"})'
    tx.run(str_merge)
    command = str_match + ', (d:Doctor)\nCREATE (a)-[:COMPANION_OF]->(d)'
    tx.run(command)
    for key in companion.keys():
        # if type(companion[key]) != list:
        #     command += key + ': ' + "'" + companion[key] + "'"
        if key == "planet":
            str_node_create_if_need = 'MERGE (p:Planet {name: "' + companion[key] + '"})\n'
            command = str_match + str_node_create_if_need + "MERGE (a) -[:COMES_FROM]->(p)"
            tx.run(command)
            #tx.run("MATCH (a:Companion {name: $name})\nMERGE (a) -[:COMES_FROM]->(p:Planet {name: $planet})", name = companion["name"], planet=companion[key])
        elif key == "actor/actress":
            str_node_create_if_need = 'MERGE (ac:Actor {name: "' + companion[key] + '"})\n'
            command = str_match + str_node_create_if_need + "MERGE (a) -[:PLAYED_BY]->(ac)"
            tx.run(command)
            # tx.run("MATCH (a:Companion {name: $name})\nMERGE (a) -[:PLAYED_BY]->(ac:Actor {name: $actor})", name = companion["name"], actor=companion[key])
        elif key == "belonings":
            for b in companion[key]:
                str_node_create_if_need = 'MERGE (b:Beloning {name: "' + b + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (a) -[:HAS]->(b)"
                tx.run(command)
                #tx.run("MATCH (a:Companion {name: $name})\nMERGE (a) -[:HAS]->(b:Beloning {name: $beloning})", name = companion["name"], beloning=b)
        elif key == "childOf":
            for p in companion[key]:
                str_node_create_if_need = 'MERGE (c:Companion{name: "'+ p +'"})\n' #to c jest robocze
                command = str_match + str_node_create_if_need + "MERGE (a) -[:CHILD_OF]->(c)" 
                tx.run(command)
                #tx.run("MATCH (a:Companion {name: $name})\nMERGE (a) -[:CHILD_OF]->(c:Ally {name: $parent})", name = companion["name"], parent=p) #robocze
        elif key == "species":
            for spec in companion[key]:
                str_node_create_if_need = 'MERGE (s:Species {name: "' + spec + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (a) -[:IS_A]->(s)"
                tx.run(command)
                #tx.run("MATCH (a:Companion {name: $name})\nMERGE (a) -[:IS_A]->(s:Species {name: $species})", name = companion["name"], species=spec)
        elif key == "firstAppearedIn":
            str_node_create_if_need = 'MERGE (ep:Episode {number: "' + companion[key]["number"]+ '", nameOfEpisode: "' +companion[key]["nameOfEpisode"] +'"})\n'
            command = str_match + str_node_create_if_need + "MERGE (a) -[:FIRST_APPEARED_IN]->(ep)"
            tx.run(command)
            #tx.run("MATCH (a:Companion {name: $name})\nMERGE MERGE (a) -[:FIRST_APPEARED_IN]->(ep)", name = companion["name"], nr=companion[key]["number"], ep_name=companion[key]["nameOfEpisode"])
        elif key == "diedIn":
            str_node_create_if_need = 'MERGE (ep:Episode {number: "' + companion[key]["number"]+'", nameOfEpisode: "' +companion[key]["nameOfEpisode"] +'"})\n'
            command = str_match + str_node_create_if_need + "MERGE (a) -[:DIED_IN]->(ep)"
            tx.run(command)
            #tx.run("MATCH (a:Companion {name: $name})\nMERGE (ep:Episode {number: $nr, nameOfEpisode : $ep_name})\nMERGE (a) -[:DIED_IN]->(ep)", name = companion["name"], nr=companion[key]["number"], ep_name=companion[key]["nameOfEpisode"])
            print(companion["name"], companion[key]["number"], companion[key]["nameOfEpisode"])
        elif key == "regenerations":
            for actor in companion[key]:
                str_node_create_if_need = 'MERGE (b:Actor {name: "' + actor["actor/actress"] + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (a) <-[:REGENERATION]-(b)"
                tx.run(command)
                #tx.run("MERGE (a:companion {name: $name1})\n MERGE (a) <-[:REGENERATION] - (b:Actor {name: $name2})", name1= companion["name"], name2 = actor["actor/actress"])
                #tx.run("MATCH (a:Companion {name: $name1})\nMERGE (a) <-[:REGENERATION] - (b:Actor {name: $name2})", name1= companion["name"], name2 = actor["actor/actress"])

def create_doctor(tx, doctor):

    tx.run("MERGE (d:Doctor {name: $name})", name = doctor["name"])

    for key in doctor.keys():
        # if type(doctor[key]) != list:
        #     command += key + ': ' + "'" + doctor[key] + "'"
        if key == "planet":
            tx.run("MATCH (a:Doctor {name: $name})\nMERGE (a) -[:COMES_FROM]->(p:Planet {name: $planet})", name = doctor["name"], planet=doctor[key])
        elif key == "actor/actress":
            tx.run("MATCH (a:Doctor {name: $name})\nMERGE (a) -[:PLAYED_BY]->(ac:Actor {name: $actor})", name = doctor["name"], actor=doctor[key])
        elif key == "belonings":
            for b in doctor[key]:
                tx.run("MATCH (a:Doctor {name: $name})\nMERGE (b:Beloning {name: $beloning})\nMERGE (a) -[:HAS]->(b)", name = doctor["name"], beloning=b)
        elif key == "species":
            for spec in doctor[key]:
                tx.run("MATCH (a:Doctor {name: $name})\nMERGE (a) -[:IS_A]->(s:Species {name: $species})", name = doctor["name"], species=spec)
        elif key == "firstAppearedIn":
            tx.run("MATCH (a:Doctor {name: $name})\nMERGE (a) -[:FIRST_APPEARED_IN]->(ep:Episode {number: $nr, nameOfEpisode : $ep_name})", name = doctor["name"], nr=doctor[key]["number"], ep_name=doctor[key]["nameOfEpisode"])
        elif key == "diedIn":
            tx.run("MATCH (a:Doctor {name: $name})\nMERGE (a) -[:DIED_IN]->(ep:Episode {number: $nr, nameOfEpisode : $ep_name})", name = doctor["name"], nr=doctor[key]["number"], ep_name=doctor[key]["nameOfEpisode"])
        elif key == "regenerations":
            for actor in doctor[key]:
                #tx.run("MERGE (a:doctor {name: $name1})\n MERGE (a) <-[:REGENERATION] - (b:Actor {name: $name2})", name1= doctor["name"], name2 = actor["actor/actress"])
                tx.run("MATCH (a:Doctor {name: $name1})\nMERGE (a) <-[:REGENERATION] - (b:Actor {name: $name2, year: $year})", name1= doctor["name"], name2 = actor["actor/actress"], year = actor["year"])

def create_season(tx, season):
    str_merge = 'MERGE (s:Season {name: "' + (list(season.keys()))[0] + '"})\n'
    se_match = 'MATCH (s:Season {name: "' + (list(season.keys()))[0] + '"})\n'

    tx.run(str_merge)
    for ep in season[(list(season.keys()))[0]]:
        create_episode(tx, ep, se_match)

def create_episode(tx, ep, se_match):
    str_match = 'MATCH (ep:Episode {number: "' + ep["episode"] + '"})\n'

    tx.run('CREATE (ep:Episode {number: $nr, title: $title})', nr=ep["episode"], title=ep["title"])
    command = str_match + se_match + 'CREATE (ep)-[:EPISODE_OF]->(s)'
    tx.run(command)

    for key in ep.keys():
        if key == "companion":
            for comp in ep[key]:
                str_node_create_if_need = 'MERGE (c:Companion {name: "' + comp + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (ep) <-[:COMPANION_IN]-(c)"
                tx.run(command)
        elif key == "enemy":
            for en in ep[key]:
                str_node_create_if_need = 'MERGE (e:Enemy {name: "' + en + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (ep) <-[:ENEMY_IN]-(e)"
                tx.run(command)
        elif key == "doctor":
            for act in ep[key]:
                str_node_create_if_need = 'MERGE (a:Actor {name: "' + act + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (ep) <-[:PLAYED_DOCTOR_IN]-(a)"
                tx.run(command)
        elif key == "allies":
            for a in ep[key]:
                str_node_create_if_need = 'MERGE (a:Ally {name: "' + a + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (ep) <-[:ALLY_IN]-(a)"
                tx.run(command)
        elif key == "enemySpecies":
            for es in ep[key]:
                str_node_create_if_need = 'MERGE (es:Species {name: "' + es + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (ep) <-[:ENEMY_SPECIES_IN]-(es)"
                tx.run(command)
        elif key == "others":
            for ot in ep[key]:
                str_node_create_if_need = 'MERGE (ot {name: "' + ot + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (ep) <-[:GUEST_IN]-(ot)"
                tx.run(command)     
        elif key == "alliedSpecies":
            for asp in ep[key]:
                str_node_create_if_need = 'MERGE (as:Species {name: "' + asp + '"})\n'
                command = str_match + str_node_create_if_need + "MERGE (ep) <-[:ALLIED_SPECIES_IN]-(as)"
                tx.run(command)   

#---------------------------------------------------------------------

#uri = "bolt://localhost:7687"
password = "storms-alternative-overtime"
driver = GraphDatabase.driver(uri, auth=("neo4j", password))

with open("planets.json", mode="r", encoding="utf-8") as file:
    planets = json.load(file)

with open("edited_characters.json", mode="r", encoding="utf-8") as file:
    characters = json.load(file)

with open("series.json", mode="r", encoding="utf-8") as file:
    series = json.load(file)

with driver.session() as session:

        for node in planets["Planets"]:
            session.execute_write(create_planet, node)

        session.execute_write(create_doctor, characters[0])

        for node in characters[1]["enemies"]:
            session.execute_write(create_enemy, node)

        for node in characters[2]["allies"]:
            session.execute_write(create_ally, node)

        for node in characters[3]["companions"]:
            session.execute_write(create_companion, node)

        for node in series:
            session.execute_write(create_season, node)
        
driver.close()
