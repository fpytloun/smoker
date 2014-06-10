#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2007-2012, GoodData(R) Corporation. All rights reserved

import logging
lg = logging.getLogger('smokerd.pluginmanager')

from smoker.server.exceptions import *
from smoker.server.plugins import Plugin

import os
import time

import multiprocessing

# Initialize threading semamphore, by default limit by
# number of online CPUs + 2
semaphore_count = int(os.sysconf('SC_NPROCESSORS_ONLN')) + 2
lg.info("Plugins will run approximately at %s threads in parallel" % semaphore_count)
semaphore = multiprocessing.Semaphore(semaphore_count)


class PluginManager(object):
    """
    PluginManager provides management and
    access to plugins
    """
    # Configured plugins/templates/actions
    conf_plugins   = None
    conf_actions   = None
    conf_templates = None

    # Plugin objects
    plugins = {}

    # Processes
    processes = []
    # We don't want to have process ID 0 (first index)
    # so fill it by None
    processes.append(None)

    def __init__(self, plugins=None, actions=None, templates=None):
        """
        PluginManager constructor
         * load plugins/templates/actions configuration
         * create plugins objects
        """
        self.conf_plugins   = plugins
        self.conf_actions   = actions
        self.conf_templates = templates

        self.stopping = False

        # Load Plugin objects
        self.load_plugins()

    def start(self):
        """
        Start all plugins
        """
        for name, plugin in self.plugins.iteritems():
            plugin.start()

    def stop(self, blocking=True):
        """
        Stop all plugins
        Wait until they are stopped if blocking=True
        """
        self.stopping = True

        # Stop of all plugins
        for name, plugin in self.plugins.iteritems():
            plugin.terminate()

        # Wait until all plugins are stopped
        if blocking:
            plugins_left = self.plugins.keys()
            plugins_left_cnt = len(plugins_left)
            while plugins_left:
                plugins_left = []
                for name, plugin in self.plugins.iteritems():
                    if plugin.is_alive():
                        plugins_left.append(name)
                if plugins_left:
                    # Print info only if number of left plugins changed
                    if len(plugins_left) != plugins_left_cnt:
                        lg.info("Waiting for %s plugins to shutdown: %s" % (len(plugins_left), ','.join(plugins_left)))
                    plugins_left_cnt = len(plugins_left)
                    time.sleep(0.5)

    def load_plugins(self):
        """
        Create Plugin objects
        """
        # Check if BasePlugin template is present
        # or raise exception
        try:
            self.get_template('BasePlugin')
        except (TemplateNotFound, NoTemplatesConfigured):
            lg.error("Required BasePlugin template is not configured!")
            raise BasePluginTemplateNotFound

        for plugin, options in self.conf_plugins.iteritems():
            if options.has_key('Enabled') and options['Enabled'] == False:
                lg.info("Plugin %s is disabled, skipping.." % plugin)
                continue

            try:
                self.plugins[plugin] = self.load_plugin(plugin, options)
            except TemplateNotFound:
                lg.error("Can't find configured template %s for plugin %s, plugin not loaded" % (options['Template'], plugin))
                continue
            except NoTemplatesConfigured:
                lg.error("There are no templates configured, template %s is required by plugin %s, plugin not loaded" % (options['Template'], plugin))
                continue
            except AssertionError as e:
                lg.error("Plugin %s not loaded: AssertionError, %s" % (plugin, e))
                continue
            except Exception as e:
                lg.error("Plugin %s not loaded: %s" % (plugin, e))
                lg.exception(e)
                continue
            lg.info("Loaded plugin %s" % plugin)

        if len(self.plugins) == 0:
            lg.error("No plugins loaded!")
            raise NoRunningPlugins("No plugins loaded!")

    def load_plugin(self, plugin, options):
        """
        Create and return Plugin object
        """
        # Load BasePlugin template first
        try:
            template = self.get_template('BasePlugin')
        except:
            template = {}

        # Plugin has template, load it's parent params
        if options.has_key('Template'):
            template_custom = self.get_template(options['Template'])
            template = dict(template, **template_custom)

        if options.has_key('Action'):
            options['Action'] = self.get_action(options['Action'])

        params = dict(template, **options)
        return Plugin(self, plugin, params, semaphore)

    def get_template(self, name):
        """
        Return template parameters
        """
        if not isinstance(self.conf_templates, dict):
            raise NoTemplatesConfigured

        try:
            params = self.conf_templates[name]
        except KeyError:
            raise TemplateNotFound("Can't find configured template %s" % name)

        return params

    def get_action(self, name):
        """
        Return template parameters
        """
        if not isinstance(self.conf_actions, dict):
            raise NoActionsConfigured

        try:
            params = self.conf_actions[name]
        except KeyError:
            raise ActionNotFound("Can't find configured action %s" % name)

        return params

    def get_plugins(self, filter=None):
        """
        Return all plugins or filter them by parameter
        """

        if filter:
            plugins = []
            key = filter.keys()[0]
            value = filter[key]

            for plugin in self.plugins.itervalues():
                if plugin.params.has_key(key):
                    if plugin.params[key] == value:
                        plugins.append(plugin)
            return plugins
        else:
            return self.plugins

    def get_plugin(self, name):
        """
        Return single plugin
        """
        try:
            return self.plugins[name]
        except KeyError:
            raise NoSuchPlugin("Plugin %s not found" % name)

    def add_process(self, plugins=None, filter=None):
        """
        Add process and force plugin run
        """
        plugins_list = []

        # Add plugins by name
        if plugins:
            for plugin in plugins:
                plugins_list.append(self.get_plugin(plugin))

        # Add plugins by filter
        if filter:
            plugins_list.extend(self.get_plugins(filter))

        # Raise exception if no plugins was found
        if len(plugins_list) == 0:
            raise NoPluginsFound

        process = {
            'plugins' : plugins_list,
        }

        plugins_name = []
        for p in plugins_list:
            plugins_name.append(p.name)

        lg.info("Forcing run of %d plugins: %s" % (len(plugins_list), ', '.join(plugins_name)))

        # Add process into the list
        self.processes.append(process)
        id = len(self.processes)-1

        # Force run for each plugin and clear forced_result
        for plugin in plugins_list:
            plugin.force = True
            plugin.forced_result = None

        return id

    def get_process(self, id):
        """
        Return process
        """
        return self.processes[id]
