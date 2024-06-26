<?php
/**
 * Zend Framework
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://framework.zend.com/license/new-bsd
 * If you did not receive a copy of the license and are unable to
 * obtain it through the world-wide-web, please send an email
 * to license@zend.com so we can send you a copy immediately.
 *
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Table
 * @copyright  Copyright (c) 2005-2015 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 * @version    $Id$
 */

/**
 * @see Zend_Db_Table_Abstract
 */


/**
 * @see Zend_Db_Table_Definition
 */


/**
 * Class for SQL table interface.
 *
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Table
 * @copyright  Copyright (c) 2005-2015 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */
class Zend_Db_Table extends Zend_Db_Table_Abstract
{

    /**
     * __construct() - For concrete implementation of Zend_Db_Table
     *
     * @param string|array $config string is the name of a table
     * @param array|Zend_Db_Table_Definition $definition
     */
    public function __construct($config = [], $definition = null)
    {
        if ($definition !== null && is_array($definition)) {
            $definition = new Zend_Db_Table_Definition($definition);
        }

        if (is_string($config)) {
            // process this as table with or without a definition
            if ($definition instanceof Zend_Db_Table_Definition && $definition->hasTableConfig($config)) {
                // this will have DEFINITION_CONFIG_NAME & DEFINITION
                $config = $definition->getTableConfig($config);
            } else {
                $config = [self::NAME => $config];
            }
        }

        parent::__construct($config);
    }
}
