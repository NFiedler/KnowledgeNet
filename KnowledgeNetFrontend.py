#! /usr/bin/env python
from MongoBackend import *
from bson import ObjectId
import argparse
import sys
import os


class KnowledgeNetFrontend:
    def __init__(self):
        self.rows, self.columns = os.popen('stty size', 'r').read().split()

        self.backend = b
        parser = argparse.ArgumentParser(prog='PROG')
        # parser.add_argument('--foo', action='store_true', help='foo help')
        subparsers = parser.add_subparsers(help='command groups')

        # create the parser for the "node" command group
        ################################################

        parser_node = subparsers.add_parser('node', help='node related commands')
        node_subparsers = parser_node.add_subparsers()

        # create a parser for the "node list" command
        parser_node_list = node_subparsers.add_parser('list', help='List all nodes')
        parser_node_list.set_defaults(func=self.list_nodes)

        # create a parser for the "node create" command
        parser_node_create = node_subparsers.add_parser('create', help='Create a new node')
        parser_node_create.add_argument('node_name', default='', nargs='?')
        parser_node_create.set_defaults(func=self.create_node)

        # info a parser for the "node info" command
        parser_node_info = node_subparsers.add_parser('info', help='Information on a node')
        parser_node_info.add_argument('node_name', default='', nargs='?')
        parser_node_info.set_defaults(func=self.node_info)

        # relations a parser for the "node relations" command
        parser_node_relations = node_subparsers.add_parser('relations', help='Information on a node')
        parser_node_relations.add_argument('node_name', default='', nargs='?')
        node_relations_dir_ndir_group = parser_node_relations.add_mutually_exclusive_group()
        node_relations_dir_ndir_group.add_argument('--dir', action='store_true', default=False)
        node_relations_dir_ndir_group.add_argument('--ndir', action='store_true', default=False)
        parser_node_relations.set_defaults(func=self.node_relations)

        # create the parser for the "relation" command group
        ####################################################
        parser_relation = subparsers.add_parser('relation', help='relation related commands')
        relation_subparsers = parser_relation.add_subparsers()

        # create a parser for the "relation create" command
        parser_relation_create = relation_subparsers.add_parser('create', help='Create a new relation')
        parser_relation_create.add_argument('relation_name', default='', nargs='?')
        relation_uni_bi_group = parser_relation_create.add_mutually_exclusive_group()
        relation_uni_bi_group.add_argument('--uni', action='store_true', default=False)
        relation_uni_bi_group.add_argument('--bi', action='store_true', default=False)
        parser_relation_create.set_defaults(func=self.create_relation)

        # create the parser for the "relationtype" command group
        ########################################################
        parser_relationtype = subparsers.add_parser('relationtype', help='relation type related commands')
        relationtype_subparsers = parser_relationtype.add_subparsers()

        # create a parser for the "relation type create" command
        parser_relationtype_create = relationtype_subparsers.add_parser('create', help='Create a new relation type')
        parser_relationtype_create.add_argument('relation_type_name', default='', nargs='?')
        relationtype_create_uni_bi_group = parser_relationtype_create.add_mutually_exclusive_group()
        relationtype_create_uni_bi_group.add_argument('--uni', action='store_true', default=False)
        relationtype_create_uni_bi_group.add_argument('--bi', action='store_true', default=False)
        parser_relationtype_create.set_defaults(func=self.create_relationtype)

        # list a parser for the "relation type list" command
        parser_relationtype_list = relationtype_subparsers.add_parser('list', help='Create a new relation type')
        parser_relationtype_list.add_argument('--uni', action='store_true', default=True)
        parser_relationtype_list.add_argument('--bi', action='store_true', default=True)
        parser_relationtype_list.set_defaults(func=self.list_relationtypes)

        
        
        # do stuff
        args = parser.parse_args()
        args.func(args)

    def list_nodes(self, args):
        print(self.backend.get_all_node_names())

    def list_relationtypes(self, args):
        if args.uni:
            print('Unidirectional relation types:')
            self.print_line('-')
            for uni_type in self.backend.get_uni_relationtypes():
                print(f'{uni_type["name"]}: {uni_type["description"]}')
            print()
        if args.bi:
            print('Bidirectional relation types:')
            self.print_line('-')
            for bi_type in self.backend.get_bi_relationtypes():
                print(f'{bi_type["name"]}: {bi_type["description"]}')

    def create_node(self, args):
        name = str(args.node_name)
        print(name)
        if name == '':
            name = input('Enter the name of the new node: ')
        description = ''
        if self.yes_no('Do you want to add a description? '):
            description = input('Enter the description of the new node: ')
        result = self.backend.add_node(name, description)
        print(result)

    def create_relationtype(self, args):
        uni, bi = args.uni, args.bi
        if uni == bi:
            uni = self.uni_bi('Enter \"uni\" or \"bi\" to create a unidirectional or a bidirectional relation type: ')
            bi = not uni

        name = str(args.relation_type_name)
        if name == '':
            name = input('Enter the name of the new {}directional relation type: '.format('uni' if uni else 'bi'))
        description = ''
        if self.yes_no('Do you want to add a description? '):
            description = input('Enter the description of the new relation type: ')
        if uni:
            result = self.backend.add_uni_rel_type(name, description)
        else:
            result = self.backend.add_bi_rel_type(name, description)
        print(result)

    def node_info(self, args):
        node = str(args.node_name)
        if node == '':
            node = input('Which node do you want to learn about? ')
        # handle follows
        while node is not None:
            node = self.to_node(node)
            node = self.node_detail(node)

    def node_relations(self, args):
        node = str(args.node_name)
        if node == '':
            node = input('Which node do you want to learn about? ')
        node = self.to_node(node)
        relation_type_name = input('What is the relation type? ')
        uni, relation_type_id = self.find_relationtype_id(relation_type_name)
        if uni:
            directed, not_directed = args.dir, args.ndir
            if directed == not_directed:
                directed = self.yes_no('Do you want to consider the relation direction? ')
            node_ids = self.backend.get_all_uni_connected_nodes(node['_id'], relation_type_id, include_direction=directed)
        else:
            node_ids = self.backend.get_all_bi_connected_nodes(node['_id'], relation_type_id)
        print([node['name'] for node in self.backend.get_nodes(node_ids)])

    def node_detail(self, node):
        self.print_line()
        print(node['name'])
        self.print_line('-')
        relation_info = self.backend.get_relation_info_of_node(node['_id'])

        relation_counter = 0
        related_node_ids = [rel['from']['_id'] for rel in relation_info['in_relations']] + \
                           [rel['to']['_id'] for rel in relation_info['out_relations']] + \
                           [rel['with']['_id'] for rel in relation_info['bi_relations']]
        for key in node.keys():
            if 'relations' not in key:
                print('{}: {}'.format(key, node[key]))
        if relation_info['in_relations']:
            print('in_relations: ')
            for relation in relation_info['in_relations']:
                print('\t{} - \"{}\" from \"{}\"'.format(relation_counter, relation['type']['name'], relation['from']['name']))
                relation_counter += 1
        if relation_info['out_relations']:
            print('out_relations: ')
            for relation in relation_info['out_relations']:
                print('\t{} - \"{}\" to \"{}\"'.format(relation_counter, relation['type']['name'], relation['to']['name']))
                relation_counter += 1
        if relation_info['bi_relations']:
            print('bi_relations: ')
            for relation in relation_info['bi_relations']:
                print('\t{} - \"{}\" with \"{}\"'.format(relation_counter, relation['type']['name'], relation['with']['name']))
                relation_counter += 1
        if relation_counter == 0:
            return None
        selection = input('If you want to follow a relation, enter the number in front of it. Otherwise type \'q\'.\n')
        while True:
            selection = selection.lower()
            if selection == 'q':
                return None
            if selection.isdigit() and int(selection) <= relation_counter:
                return related_node_ids[int(selection)]
            selection = input('Your input was invalid. If you want to follow a relation, enter the number in front of it. Otherwise type \'q\'.\n')
            
    def create_relation(self, args):
        uni, bi = args.uni, args.bi
        if uni == bi:
            uni, rel_type = self.find_relationtype_id(input('Enter the name of the relation type: '))
        else:
            rel_type = self.to_relationtype_id(input('Enter the name of the relation type: '), uni=uni)
        node1 = self.to_node_id(input('Enter the name of the first (origin) node: '))
        node2 = self.to_node_id(input('Enter the name of the second (target) node: '))
        if uni:
            result = self.backend.add_uni_relation(node1, node2, rel_type)
        else:
            result = self.backend.add_bi_relation(node1, node2, rel_type)
        print(result)

    def to_node(self, name, exit_on_err=True):
        if type(name) == ObjectId:
            return self.backend.get_node(name)
        nodes = self.backend.list_nodes_by_name(name)
        if not nodes:
            print('There is no node known by the name \"{}\"'.format(name))
            if exit_on_err:
                print('Exiting...')
                sys.exit()
            return None
        if len(nodes) == 1:
            return nodes[0]

        # Nodes is long. This means that there are multiple nodes by that name
        return nodes[int(self.select_node(nodes))]

    def to_node_id(self, name, exit_on_err=True):
        if type(name) == ObjectId:
            return name
        nodes = self.backend.list_nodes_by_name(name)
        if not nodes:
            print('There is no node known by the name \"{}\"'.format(name))
            if exit_on_err:
                print('Exiting...')
                sys.exit()
            return None
        if len(nodes) == 1:
            return nodes[0]['_id']

        # Nodes is long. This means that there are multiple nodes by that name
        return nodes[int(self.select_node(nodes))]['_id']

    def find_relationtype_id(self, name, exit_on_err=True):
        if type(name) == ObjectId:
            return name
        types = self.backend.list_relationtypes_by_name(name, uni=True)
        uni_count = len(types)
        types += self.backend.list_relationtypes_by_name(name, uni=False)
        if not types:
            print('There is no relation type known by the name \"{}\"'.format(name))
            if exit_on_err:
                print('Exiting...')
                sys.exit()
            return None, None
        if len(types) == 1:
            return (uni_count > 0), types[0]['_id']

        # Nodes is long. This means that there are multiple nodes by that name
        uni, selection = self.select_relationtype(types, uni_count)
        return uni, types[selection]['_id']

    def to_relationtype_id(self, name, uni, exit_on_err=True):
        if type(name) == ObjectId:
            return name
        types = self.backend.list_relationtypes_by_name(name, uni=uni)
        if not types:
            print('There is no relation type known by the name \"{}\"'.format(name))
            if exit_on_err:
                print('Exiting...')
                sys.exit()
            return None
        if len(types) == 1:
            return types[0]['_id']

        # Nodes is long. This means that there are multiple nodes by that name
        return types[int(self.select_relationtype(types))]['_id']

    def select_node(self, nodes):
        print('There exist multiple nodes with the name \"{}\": '.format(nodes[0]['name']))
        for i, node in enumerate(nodes):
            print('{} - {}: {}'.format(i, node['name'], node['description']))
        selection = ''
        while not selection.isdigit() or len(nodes) < int(selection):
            selection = input('Enter your selection: ')
        return selection

    def select_relationtype(self, types, uni_count=-1):
        print('There exist multiple relation types with the name \"{}\": '.format(types[0]['name']))
        rel_type_group = ''
        for i, type in enumerate(types):
            if uni_count >= 0:
                if i < uni_count:
                    rel_type_group = '[uni]'
                    usage_count = ' (used in {} relations)'.format(self.backend.get_uni_relationtype_usage_number(type['_id']))
                else:
                    rel_type_group = '[bi]'
                    usage_count = ' (used in {} relations)'.format(self.backend.get_bi_relationtype_usage_number(type['_id']))

            print('{} - {} {}: {}{}'.format(i, type['name'], rel_type_group, type['description'], usage_count))
        selection = ''
        while not selection.isdigit() or len(types) < int(selection):
            selection = input('Enter your selection: ')
        selection = int(selection)
        if uni_count >= 0:
            return (selection < uni_count), selection
        return selection

    def yes_no(self, answer):
        yes = {'yes', 'y', 'ye', ''}
        no = {'no', 'n'}

        while True:
            choice = input(answer).lower()
            if choice in yes:
                return True
            elif choice in no:
                return False
            else:
                print('Please respond with \'yes\' or \'no\'')

    def uni_bi(self, answer):
        uni = {'u', 'uni'}
        bi = {'b', 'bi'}

        while True:
            choice = input(answer).lower()
            if choice in uni:
                return True
            elif choice in bi:
                return False
            else:
                print('Please respond with \'uni\' or \'bi\'')

    def print_line(self, char='#'):
        print(char * int(self.columns))


twf = KnowledgeNetFrontend()
