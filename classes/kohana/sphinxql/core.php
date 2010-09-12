<?php defined('SYSPATH') or die('No direct script access.');

/**
 * This file is part of SphinxQL for Kohana.
 *
 * Copyright (c) 2010, Deoxxa Development
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * @package kohana-sphinxql
 */

/**
 * Class for managing a set of SphinxQL_Clients
 *
 * @package kohana-sphinxql
 * @author MasterCJ <mastercj@mastercj.net>
 * @version 0.1
 * @license http://mastercj.net/license.txt
 */
class Kohana_SphinxQL_Core {
	/**
	 * @var array A collection of SphinxQL_Clients
	 */
	protected static $_handles = array();

	/**
	 * Constructor
	 *
	 * @param string The profile name, corresponds to the config file
	 * @param array Config settings to override defaults
	 */
	public function __construct($profile = 'default', array $config = NULL)
	{
		if ($config === NULL)
		{
		    $config = array();
		}
		$config = Arr::merge(Kohana::config('sphinxql.'.$profile), $config);
		
		foreach ($config['servers'] as $name => $server)
		{
		    $this->add_server($name, $server);
		}
	}

	/**
	 * Create a new SphinxQL_Client for a server and add it to the pool of clients
	 *
	 * @param string An alias for this server
	 * @param string The address and port of a server
	 * @return boolean The status of the creation of the SphinxQL_Client
	 */
	public function add_server($name, $server)
	{
		if (is_string($server))
		{
			if (isset(self::$_handles[$server]))
			{
			    return TRUE;
			}
			if ($client = new SphinxQL_Client($server))
			{
			    self::$_handles[$name] = $client;
			    return TRUE;
			}
		}

		return FALSE;
	}

	/**
	 * Create a new SphinxQL_Query, automatically add this SphinxQL_Core as the constructor argument
	 *
	 * @return SphinxQL_Query|false The resulting query or false on error
	 */
	public function new_query()
	{
		if ($query = new SphinxQL_Query($this))
		{
		    return $query;
		}
		
		return FALSE;
	}

	/**
	 * Perform a query, given either a string or a SphinxQL_Query object
	 * Cycles through all available servers until it succeeds.
	 * In the event that it can't find a responsive server, returns false.
	 *
	 * @param SphinxQL_Query|string A query as a string or a SphinxQL_Query object
	 * @return array|false The result of the query or false
	 */
	public function query($query)
	{
		if (!is_a($query, 'SphinxQL_Query') AND !is_string($query))
		{
		    return FALSE;
		}
		
		while (($names = array_keys(self::$_handles)) AND 
		        count($names) AND ($name = $names[intval(rand(0, count($names)-1))]))
	    {
			$client = self::$_handles[$name];
			
			$return = $client->query( (string) $query)->fetch_all();
			
			if (is_array($return))
			{
			    $info = $client->query('SHOW META')->fetch_all();
			    
			    $result = array();
			    foreach($info as $info_row)
			    {
			        // parse META info (http://www.sphinxsearch.com/docs/current.html#sphinxql-show-meta)
			        preg_match('/^(keyword|docs|hits)\[(\d)\]$/', $info_row['Variable_name'], $matches);
			        
			        if (isset($matches[1]))
			        {
			            switch($matches[1])
			            {
			                case 'keyword':
			                    $result['words'][$info_row['Value']] = array();
			                    $last_word = $info_row['Value'];
			                break;
			                case 'hits':
			                    $result['words'][$last_word]['hits'] = $info_row['Value'];
			                break;
			                case 'docs':
			                    $result['words'][$last_word]['docs'] = $info_row['Value'];
			                break;
			            }   
			        }
			        else
			        {
			            $result[$info_row['Variable_name']] = $info_row['Value'];
			        }
			    }
			    
			    $result['matches'] = $return;
			    
			    return $result;
			}
			else
			{
			    unset(self::$_handles[$name]);
			}
		}
		
		return FALSE;
	}
}

