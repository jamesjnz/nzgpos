<?php


namespace nz\gpos\common\orm;



class Gpos_Orm  extends \Zend_Db_Table_Abstract{

    // we need a primary key
    // and an array of other fields.. and a mapping to an internal field..
    // much like the cmskvc but lighter..


    public $primaryKey;
    public $identifyingKeys;

    public $tableFields;

    public $tableFieldTypes;//the data type

    public $tableFieldLabels;
    public $tableFieldReadOnly;

    public $tableRequiredFields;
    public $tableRelationships;//keys that are actually objects.. so we can do stuff

    public $tableRelationshipClasses;

    //introspected from the class???
    public $tableName;

    public $uniqueKeys;

//		protected $data;

    public $errors;//needs

    protected $dataHasChanged;//

    protected $_tablePrefix;

    private $_currentIteratorKey;
    private $_iteratorKeys;

    function __construct (\Zend_Db_Adapter_Abstract $zdb=null){
        parent::__construct($zdb);

        //	$this->primaryKey = "";
        //	$this->tableName = "";
        //	$this->_tablePrefix = "";


//			$this->data = array();
        $this->identifyingKeys =array();
        $this->uniqueKeys = array();
        $this->tableFields = array();
        $this->tableFieldTypes = array();
        $this->tableRequiredFields = array();
        $this->tableRelationships = array();
        $this->tableRelationshipClasses = array();
        $this->tableFieldLabels = array();
        $this->tableFieldReadOnly = array();

        //set these defaults here and let them get overwritten..

        //make a mechanism here to cache the table structures and what not..
        // could read from either code.. or a file.. or the databases table metadta..

        // at this point.. we will just loop over the keys.. and add them to the data array
        // but with null values..
        $this->dataHasChanged = false;
    }


    function getTableBaseName(){
        return $this->tableName;
    }

    function getTablePrefix(){
        return $this->_tablePrefix;
    }

    /**
     * Gets the primary key value from the objects
     */
    function getPrimaryKey(){
        $pk = $this->primaryKey;
        if (isset( $this->{ $pk })) {
            return $this->{ $pk };
        }

        return null;
    }

    /**
     *
     * Sets the primary key for the object
     * @param mixed $primaryKey
     */
    function setPrimaryKey($primaryKey){
        $this->{ $this->primaryKey } = $primaryKey;
    }

    /**
     * returns a list of fields that will be saved when save is called
     */
    protected function _tableFieldsToSave(){
        return $this->tableFields;
    }


    /**
     * returns a dictionary of relationships that will be saved out when save is called
     * Over ride this in subclasses to prevent the ORM automatically saving on records you
     * do not want it to save.
     */
    protected function _tableRelationshipsToSave() {
        return $this->tableRelationships;
    }


    /**
     * returns an array of data that will be persisted when save is called, this data should be mapped
     * to match the fields of the underlying database structure.
     */
    protected function _dataForInsert(){
        //write data ..
        $data = array();
        $fieldsToSave = $this->_tableFieldsToSave();
        foreach ($fieldsToSave as $field	){
            // add additional typecasting here.. to
            // ensure type correctness.. like
            // the json type fangling.
            if (isset($this->{ $field })) {

                //$o = $this->{ $field };

                //	if ( $insert ){
                $o = $this->getFieldValueForInsert($field);
                //	} else {
                //		$o = $this->getFieldValueForUpdate($field);
                //	}

                //if we have an ftaorm object, and the field is a fk..
                //if its not.. then we assume something bad is about to happen, or the
                //object knows how to serialise out
//					// for saving to the databse
//					if ($o instanceof FTAORM && isset($this->tableFieldTypes[$field]) && $this->tableFieldTypes[$field] == 'foreignKey') {
//						$data[ $field ] = $o->getPrimaryKey();
//					} else {
                $data[ $field ] = $o;
//					}

            }
        }
        return $data;
    }

    /**
     *
     * prepares a dictionary of identifying keys and values that are used to unique an object when updating
     */
    protected function _identifyingKeysForUpdate(){
        $where = array();
        $where[$this->primaryKey ." = ? "] = $this->getFieldValueForInsert($this->primaryKey);
        foreach ($this->identifyingKeys as $key ){
            $value = $this->getFieldValueForInsert($key);
            $where[$key ." = ? "] = $value;
        }
        return $where;
    }


    /**
     * Saves / persists the object back to the datastore, all relationships that are defined will also be saved.
     * @see FTAORM/FTABaseORM::save()
     */
    function save($newPrimaryKey=null){//takes a zend database thingee

        //if we have no primary key.. then we are inserting..
        // otherwise we assume new content..
        if (!$this->isValid()){
            return false;
        }

        //are we inserting?
        $insert = !isset($this->{ $this->primaryKey } ) || is_null($this->{ $this->primaryKey });

        // roll up data for insert..
        $data = $this->_dataForInsert();

        if ( $insert ) {

            //assume that this throws an exception..
            //$data[ $this->primaryKey ] = "";
            if (!is_null($newPrimaryKey))
                $data[ $this->primaryKey ] = $newPrimaryKey; // inserts on named parameters are quoted VVVV but below in the update they need to be manually quoted .. we will never instert literals..


            //on adding we assume that the identifiying keys are included in the data..
            //
            // 2012-05-11 - JRM - If this fails check you're instansiating your ORM class with a $zdb
            //                  - dont be dumb like me.
            $this->zdb->insert($this->tableName, $data);

            if (is_null($newPrimaryKey)){
                $pk = $this->zdb->lastInsertId();
            } else {
                $pk = $newPrimaryKey;
            }

            $this->{ $this->primaryKey } = $pk;


        } else {

            //$this->getPrimaryKey()

            // UPDATING		.. we need to specify the identifing keys here..
            $where = $this->_identifyingKeysForUpdate();

            //single primary key..
            $this->zdb->update($this->tableName, $data, $where);//build a where clause... in more advanced versions

        }

        // no errors here .. return true;;
        // database will throw if there are problems.. with saving etc..

        // now the relationships for items that are 1-1

        $relationshipsToSave = $this->_tableRelationshipsToSave();

        foreach($relationshipsToSave as $relationshipName => $relationshipType){



            switch ($relationshipType) {
                case static::RELATIONSHIP_ONE_TO_ONE:


                    if (isset($this->{ "_" . $relationshipName }) && !is_null($this->{ "_" . $relationshipName })){

                        $o = $this->{ "_" . $relationshipName };

                        $o->save($this->getPrimaryKey());


                    }


                    break;

                case static::RELATIONSHIP_ONE_TO_MANY:

                    //this is like the bit below.. but we dont need to create a joiner..
                    if(isset($this->{ $relationshipName })){
                        $objects = $this->{ $relationshipName };
                        if (is_array($objects)){
                            foreach ($objects as $o){
                                $pk = $this->getPrimaryKey();
                                $o->save($pk);

                            }
                        }
                    }

                    break;

                case static::RELATIONSHIP_MANY_TO_MANY:
                    // we need to get the name of the join table, adn the name of the destination
                    // we then, 1 clean out any old

                    // get the relationship, we always assume for a m-m that
                    // we get an array of objects..

                    //first remove the old tags relationships
                    // if we are here.. I AM THE SOURCE TABLE!!
                    if (isset($this->tableRelationshipClasses[$relationshipName])){

                        // now loop over the related items..
                        // save them .. create the appropriate mtm wrapper and save that too..

                        if(isset($this->{ $relationshipName })){

                            //make sure we fault the objects before deletming them..
                            $objects = $this->{ $relationshipName };

                            //remove old ones.. we move this here, so that we only purge if we have
                            //something loaded/faulted.. as even if there are none..
                            // we should have an empty array..
                            $mtmclass = static::MANY_TO_MANY_WRAPPER;

                            $relatedClass = new $this->tableRelationshipClasses[$relationshipName]($this->zdb);
                            $mtm = new $mtmclass($this, $relatedClass, $this->zdb);

                            $this->zdb->delete($mtm->joinTableName,$mtm->sourceIdName . ' = ' . $this->getPrimaryKey());


                            if (is_array($objects)){
                                foreach ($objects as $o){
                                    $s = $o->save();
                                    if ($s) {//problem here is that we may not want to save the objects, so really, the object should decide if it wants to save or not.. not this part here..
                                        $mtm = new $mtmclass($this, $o, $this->zdb);
                                        $mtm->save();//
                                    }

                                }
                            }
                        }

                    }

                    break;

            }

        }

        $this->dataHasChanged = false;
        return true;
    }

    /**
     *
     * takes an object and maps the values from the given object into this object.
     * @param $object
     */
    function takeValuesFromObject($object){
        return $this->takeValuesFromArray( (array) $object);
    }

    /**
     * List of fields that the object is allowed to take values from when takeValuesFromArray is called
     */
    protected function _tableFieldsToTake(){
        return $this->tableFields;
    }

    /**
     * list of relationships that this object is allowed to take when takeValuesFromArray is called
     */
    protected function _tableRelationshipsToTake() {
        return $this->tableRelationships;
    }


    /**
     *
     * Takes values from an array and attempts to populate this object with the values.
     * @param $values
     */
    function takeValuesFromArray(array $values){

        $fieldsSet = array();

        //this is already kind of cleaned up.. best leave it..
        if (isset($values[ $this->primaryKey ])){
            array_push($fieldsSet, $this->primaryKey);
            $this->setPrimaryKey($values[ $this->primaryKey ]);
        }


        //
        $fieldsToTake = $this->_tableFieldsToTake();
        foreach($fieldsToTake as $fieldName){
            if (isset($values[$fieldName])){
                array_push($fieldsSet, $fieldName);
                $this->{ $fieldName } = $values[$fieldName];
            }
        }
        //

        // we dont need
        //now we need to also check for various relationships that need to be set
        $relationshipsToTake = $this->_tableRelationshipsToTake();
        foreach($relationshipsToTake as $relationshipName => $relationshipType){

            switch ($relationshipType) {
                case static::RELATIONSHIP_ONE_TO_ONE:
                    //the one to one.. cluster fuck..
                    // do we merge this shit it auto?
                    //assume that we will always have the right object
                    // or um.. we will assume that the setter/getter on the class knows what to do with the data
                    // provided.. so if it does not match.. it gets shitty?
                    // if it cant map it
                    if (isset($values[$relationshipName])) {
                        array_push($fieldsSet, $relationshipName);
                        $this->{ $relationshipName } = $values[$relationshipName];
                    }
                    break;

                case static::RELATIONSHIP_ONE_TO_MANY:
                    // same as the many to many.. in terms of setting here
                    //break;

                case static::RELATIONSHIP_MANY_TO_MANY:
                    //with many to manys, we assume that we can just call the setter and we will
                    // have the correct data provided to us, so these should either
                    // be able  to work with arrays or strings or what ever. easier than working other shit out..
                    // and then we dont need to enforce knowledge about things here
                    if (isset($values[$relationshipName])) {
                        array_push($fieldsSet, $relationshipName);
                        $this->{ $relationshipName } = $values[$relationshipName];
                    }


                    break;
            }
        }


        // do we treat this as a change? if we fire it?
        // or not
        //$this->dataHasChanged = true;//this is cheating.. we assume that this
        //not sure if we need to do anything else here..
        //return a list of what we actually change..


        return $fieldsSet;
    }

    //replaced with the setDefaults below.. we can use that to clear and set up sane default values.. or reset..
    //	function setKeysToNull(){
    //		foreach($this->tableFields as $fieldName){
    //				$this->{ $fieldName } = null;
    //		}
    //	}

    /**
     * method to set all default fields and values
     */
    public function setDefaults(){
        $fields  = $this->_tableFieldsToTake();
        foreach($fields as $fieldName){
            $this->{ $fieldName } = null;
        }
    }


    /**
     * Simple load method for loading an object from the database and setting the values.
     * @see FTAORM/FTABaseORM::load()
     */
    function load($id) {

        if (is_null($id))
            return false;

        $this->zdb->setFetchMode(Zend_Db::FETCH_ASSOC);
        $select = $this->zdb->select();

        $select->from(array('m' => $this->tableName), $this->tableFields);
        $select->where($this->primaryKey .'=?', $id);

        $statement = $select->query();
        $result = $statement->fetchAll();


        if (1 != count($result))	{
            //throw an error here.. WHEEEEE
            return null;
        }

        //check for a valid object?? nah.. should be if its loaded..
        //

        $this->data = array();
        $this->setDefaults();
        $this->takeValuesFromArray($result[0]);

        // relationships will be lazy loaded
        $this->dataHasChanged = false;
        return true;
    }

    /*
     * simple wrapper to allow the isValid to be called via the introspection/kvc interface
     */
    function getIsValid(){
        return $this->isValid();
    }


    /**
     * Checks that required fields are not null
     */
    function validateRequiredFields(){

        foreach($this->tableRequiredFields as $requiredField){
            $this->validateRequiredField($requiredField);
        }
    }

    function validateRequiredField($requiredField){

        if (isset($this->errors[$requiredField])) return;

        if ( !isset($this->{ $requiredField }) || is_null($this->{$requiredField}) || (is_string($this->{$requiredField}) && 0 >= strlen($this->{$requiredField}))
        ){
            $this->errors[$requiredField] = array(
                static::VALIDATION_FIELD_NAME => $requiredField,
                $requiredField => static::FIELD_IS_REQUIRED,
                static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::FIELD_IS_REQUIRED_MESSAGE

            );
        }
    }


    function validateUniqueFields(){

        foreach ($this->uniqueKeys as $field){
            $this->validateUniqueField($field);
        }

    }

    function validateUniqueField($field){

        if (isset($this->errors[$field])) return;

        // we build a query that checks to see if there
        // is a matching row in the database
        //

        $this->zdb->setFetchMode(Zend_Db::FETCH_ASSOC);
        $select = $this->zdb->select();

        $select->from($this->tableName, array('COUNT(' . $this->primaryKey . ') AS count' ) )->order(array( $this->primaryKey ." DESC" ))
            ->where($field.' = ?', $this->{$field} );

        if (!is_null($this->getPrimaryKey())){
            $select->where($this->primaryKey .'!=?', $this->getPrimaryKey() );
        }

        $statement = $select->query();
        $result = $statement->fetchAll();

        if (count($result) == 1 && $result[0]['count'] > 0){
            $this->errors[$field] = array(
                static::VALIDATION_FIELD_NAME => $field,
                $field => static::FIELD_IS_NOT_UNIQUE,
                static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::FIELD_IS_NOT_UNIQUE_MESSAGE
            );
        }
    }

    /**
     *
     * Enter description here ...
     * @param $field the name of the field that is being checked.
     */
    function validateFieldByMethod($field){

        if (isset($this->errors[$field])) return;

        $validationMethodName = "validate" . ucfirst( $field );
        if (method_exists($this,$validationMethodName)){
            $validationError = $this->{ $validationMethodName }();
            if ($validationError) {

                $this->errors[$field] = array(
                    static::VALIDATION_FIELD_NAME => $field,
                    static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => $validationError,
                    $field => static::FIELD_IS_INVALID
                );

            }
        }
    }

    /**
     * Performs simple validation on field using extract functions and comparison.
     * @param $field name of field to validate
     */
    function validateField($field){

        if (isset($this->errors[$field])) return;

        $ex = Extract::singleton();

        $fieldType = $ex->extractStringWithKey($field, $this->tableFieldTypes,'string');
        $extractMethod = "extract" . ucfirst($fieldType) . 'WithKey';


        //$value = $extractFunction($field, $this->data, null,true);
        switch ($fieldType){

            case 'string':
            case 'cleanString':
            case 'bigString':
                //case 'value':
                $value = $ex->{ $extractMethod }($field, $this->data,null,true);// not sure how this will work.. grumble.
                break;
            case 'password':
                $value = $ex->extractPasswordWithKey($field, $this->data,null);
                break;
            default:
                $value = $ex->{ $extractMethod }($field, $this->data,null);// not sure how this will work.. grumble.
        }

        if (is_null($value)){
            $this->errors[$field] = array(
                static::VALIDATION_FIELD_NAME => $field,
                $field => static::FIELD_IS_INVALID,
                static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::FIELD_IS_INVALID_MESSAGE
            );

        }


    }

    protected function _tableFieldsToValidate(){
        return $this->tableFields;
    }

    function validateFields(){
        $fieldsToValidate = $this->_tableFieldsToValidate();
        foreach ($fieldsToValidate as $field){
            //bit of a hack here.. but we use the extract functions to
            // take items out of the data array/object and if we get null then um
            // it is invalid.. as we are also testing that the field is set...
            //empty fields are not invalid null is also considered valid..
            // all tests to ensure the field is set are performed above.

            //echo $field .": " . $this->{ $field } ."<br/>";

            if ( isset($this->{ $field }) && !is_null($this->{ $field }) ){

                //basic validation here..
                $this->validateField($field);

                //check for additional validation fileds.. this function will introspect/make a method mane and call it if its present
                $this->validateFieldByMethod($field);


            }
        }

    }

    protected function _tableRelationshipsToValidate () {
        return $this->tableRelationships;
    }

    function validateRelationships(){


        $relationshipsToValidate = $this->_tableRelationshipsToValidate();
        foreach ($relationshipsToValidate as $relationshipName => $relationshipType ){

            $isRequired = false;

            $isRequired = in_array($relationshipName, $this->tableRequiredFields);

            //TODO: tag validation etc..
            switch ($relationshipType){

                case static::RELATIONSHIP_ONE_TO_ONE:
                case static::RELATIONSHIP_ONE_TO_MANY:

                    // we should have, with these.. a record that stores the relationship target object..
                    //if the relationship is called xx then the targetobject will be called xxObject
                    // and accessed via both the magic redirection stuff or standard get/get ie getXxObject and setXxObject

                    if (isset($this->{ "_" . $relationshipName }) && !is_null($this->{ "_" . $relationshipName  })){
                        $o = $this->{ "_" . $relationshipName };
                        if ($o instanceof ORM){
                            if (!$o->isValid()){
                                //if we are not valid. we need to push in a reference to the object
                                // and what not..

                                $this->errors[$relationshipName] = array(
                                    static::VALIDATION_FIELD_NAME=>$relationshipName,
                                    $relationshipName => static::RELATIONSHIP_IS_INVALID,
                                    static::ERRORKEY_RELATEDOBJECT => $o,
                                    static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::RELATIONSHIP_IS_INVALID_MESSAGE
                                );

                            }
                        } else if (is_array($o) && count($o) >0 ) {// is traversable???//TODO: make this check for traversable??


                            $relatedValidationErrors = array();

                            foreach ( $o as $oo) {
                                if (!$oo->isValid()){
                                    array_push(
                                        $relatedValidationErrors, $oo
                                    );
                                }
                            }

                            if (0 < count($relatedValidationErrors)){
                                $this->errors[$relationshipName] = array(
                                    static::VALIDATION_FIELD_NAME=>$relationshipName,
                                    $relationshipName => static::RELATIONSHIP_IS_INVALID,
                                    static::ERRORKEY_RELATED_MANY_TO_MANY_OBJECTS => $relatedValidationErrors,
                                    static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::RELATIONSHIP_IS_INVALID_MESSAGE
                                );
                            }
                        } else {

                            if ($isRequired){
                                $this->errors[$relationshipName] = array(
                                    static::VALIDATION_FIELD_NAME=>$relationshipName,
                                    $relationshipName => static::RELATIONSHIP_IS_REQUIRED,
                                    static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::RELATIONSHIP_IS_REQUIRED_MESSAGE

                                );
                            }

                        }

                    } else {
                        //nothing so we need to check for the field being required..
                        if ($isRequired){
                            $this->errors[$relationshipName] = array(
                                static::VALIDATION_FIELD_NAME=>$relationshipName,
                                $relationshipName => static::RELATIONSHIP_IS_REQUIRED,
                                static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::RELATIONSHIP_IS_REQUIRED_MESSAGE
                            );
                        }
                    }

                    break;



                //this is validated the same as a many to many..

                case static::RELATIONSHIP_MANY_TO_MANY:
                    // simply iterate through the various objects
                    // and validate them, we dont need to make the join stuff  up here
                    // .. or do we...

                    //the internal array is accessed directly, as to avoid any sillyness going on when the
                    // getter/setters are called, and we dont need to fault objects in if they have not changed.
                    if (isset($this->{ "_" . $relationshipName }) && is_array($this->{ "_" . $relationshipName  }) && count($this->{ "_" . $relationshipName  }) > 0 ){

                        $relatedObjects = $this->{ "_" . $relationshipName  };

                        $relatedValidationErrors = array();

                        foreach ( $relatedObjects as $o) {
                            if (!$o->isValid()){
                                array_push(
                                    $relatedValidationErrors, $o
                                );
                            }
                        }

                        if (0 < count($relatedValidationErrors)){
                            $this->errors[$relationshipName] = array(
                                static::VALIDATION_FIELD_NAME=>$relationshipName,
                                $relationshipName => static::RELATIONSHIP_IS_INVALID,
                                static::ERRORKEY_RELATED_MANY_TO_MANY_OBJECTS => $relatedValidationErrors,
                                static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::RELATIONSHIP_IS_INVALID_MESSAGE
                            );
                        }

                    } else {

                        if ($isRequired){
                            $this->errors[$relationshipName] = array(
                                static::VALIDATION_FIELD_NAME=>$relationshipName,
                                $relationshipName => static::RELATIONSHIP_IS_REQUIRED,
                                static::VALIDATION_ADDITIONAL_VALIDATION_MESSAGE => static::RELATIONSHIP_IS_REQUIRED_MESSAGE
                            );
                        }

                    }


                    break;
            }
        }
    }

    /**
     * Performs validation on target object, based on specified field types, provided validation methods and optionality of fields
     * (non-PHPdoc)
     * @see FTAORM/FTABaseORM::isValid()
     */
    function isValid(){
        // invalid fields/errors are cleared.. when this is callled
        $this->errors = array();

        // how do we validate the primary key???

        //first we check for required fields..
        // these must be set and not null..
        $this->validateRequiredFields();

        //next we check the uniqueFields
        $this->validateUniqueFields();

        //next we check the validation rules..
        $this->validateFields();

        //now validate the relationships..
        $this->validateRelationships();


        return (0 == count($this->errors));
    }


    /**
     * returns the array of validation errors
     * (non-PHPdoc)
     * @see FTAORM/FTABaseORM::getValidationErrors()
     */
    function getValidationErrors(){
        return $this->errors;
    }


//relationship helpers


    /**
     * helper method for dealing with one to one objects
     */
    function getOneToOneObjectWithNameAndClass($name,$class){

        //if we have no video bio.. and a primary key .. then we try an load.. otherwise we are null..
        if (isset($this->{ "_" . $name  })) {
            return $this->{ "_" . $name  };
        } else if (!isset($this->{ "_" . $name }) && !is_null($this->getPrimaryKey())){

            $o = new  $class($this->zdb);
            $pk = $this->getPrimaryKey();
            if ( $o->load($pk) ){
                $this->{ "_" . $name } = $o;
                return $this->{ "_" . $name };
            }
        }
        return null;
    }





    public function __get($name){


        //finally no luck, try the parent..
        return parent::__get($name);
    }


    //this is a helper class to get all objects of the given class
    // NEEDS PHP5.3
    /**
     * function to get an array of all objects of the target class.. could use lots of memory
     * Enter description here ...
     * @param Zend_Db_Adapter_Abstract $zdb
     */
    public static function getAll(Zend_Db_Adapter_Abstract $zdb){

        $myClass = get_called_class();

        $o = new $myClass($zdb);

        $zdb->setFetchMode(Zend_Db::FETCH_ASSOC);
        $select = $zdb->select();

        $select->from(array('m' => $o->tableName), $o->tableFields )
            ->order(array( $o->primaryKey ." DESC" ));


        $statement = $select->query();
        $result = $statement->fetchAll();

        return static::rowsToObjects($result,$myClass,$zdb);

    }

    public static function getAllActive(Zend_Db_Adapter_Abstract $zdb){

        $myClass = get_called_class();

        $o = new $myClass($zdb);

        $zdb->setFetchMode(Zend_Db::FETCH_ASSOC);
        $select = $zdb->select();


        $select->from(array('m' => $o->tableName), $o->tableFields )
            ->order(array( $o->primaryKey ." DESC" ));

        if ($o instanceof ActiveProperty){
            $select->where('m.active=1');
        }

        //    $query = $select->__tostring();

        $statement = $select->query();
        $result = $statement->fetchAll();

        return static::rowsToObjects($result,$myClass,$zdb);
    }

    public static function getObjectsWithIdAndForeignKeyName($id, $keyName , Zend_Db_Adapter_Abstract $zdb){
        $myClass = get_called_class();

        $o = new $myClass($zdb);

        $zdb->setFetchMode(Zend_Db::FETCH_ASSOC);
        $select = $zdb->select();


        $select->from(array('m' => $o->tableName), $o->tableFields )
            ->order(array( $o->primaryKey ." DESC" ));


        $select->where('m.' . $keyName .  '=?', array($id));

        $statement = $select->query();
        $result = $statement->fetchAll();

        return static::rowsToObjects($result,$myClass,$zdb);
    }


    public static function getActiveObjectsWithIdAndForeignKeyName($id, $keyName , Zend_Db_Adapter_Abstract $zdb){
        $myClass = get_called_class();

        $o = new $myClass($zdb);

        $zdb->setFetchMode(Zend_Db::FETCH_ASSOC);
        $select = $zdb->select();


        $select->from(array('m' => $o->tableName), $o->tableFields )
            ->order(array( $o->primaryKey ." DESC" ));

        $select->where('m.' . $keyName .  '=?', array($id));


        if ($o instanceof ActiveProperty){
            $select->where('m.active=1');
        }

        $statement = $select->query();
        $result = $statement->fetchAll();

        return static::rowsToObjects($result,$myClass,$zdb);
    }


    public static function getActiveWithId($id, Zend_Db_Adapter_Abstract $zdb = null){
        $o = static::getObjectWithId($id, $zdb);
        if ($o instanceof ActiveProperty && !$o->active){
            return null;
        }

        return $o;
    }

    /**
     * returns an array of primary keys/ identifiers for all rows in the datbase for the given object/class
     * @param Zend_Db_Adapter_Abstract $zdb
     */
    public static function getAllIds(Zend_Db_Adapter_Abstract $zdb){
        $myClass = get_called_class();
        $o = new $myClass($zdb);
        $zdb->setFetchMode(Zend_Db::FETCH_ASSOC);

        $select = $zdb->select()->from($o->tableName, array($o->primaryKey))->order(array( $o->primaryKey ." DESC" ));

        $statement = $select->query();
        $result = $statement->fetchAll();
        return $result;
    }

    /**
     *
     * takes an array of database rows and attempts to convert them in to objects of the specificed class
     * @param unknown_type $rows
     * @param unknown_type $class
     * @param Zend_Db_Adapter_Abstract $zdb
     */
    public static function rowsToObjects($rows,$class,Zend_Db_Adapter_Abstract $zdb=null){
        $objects = array();
        if (is_array($rows)){
            foreach ($rows as $row){
                $oo = new $class($zdb);
                $oo->setDefaults();
                $oo->takeValuesFromArray($row);

                $objects[ $oo->getPrimaryKey() ] = $oo;
            }
        }
        return $objects;
    }


    public static function mutateToObject($v, Zend_Db_Adapter_Abstract $zdb = null){
        $c = get_called_class();
        if (is_numeric($v)){
            return $c::getObjectWithId($v,$zdb);
        } else if ($v instanceof $c ){
            return $v;
        }
        return null;

    }
//--


    function __set($name, $value){
        $this->dataHasChanged = true;
        parent::__set($name,$value);
    }


    // Field helpers..
    //sucks valus out and gets them ready for inserting, as such, since inserts are
    //quoted when they are prepared, the return value is not quoted..

    /**
     *
     * gets a value from the object and prepares it for insertion/updating in the database
     * @param $fieldName name of field
     * @return mixed
     */
    public function getFieldValueForInsert($fieldName){

        //types taht can be literals..
        // numbers
        // boolean
        // primarykey/foreignkey
        //

        //everything else is quotoed

        //$fieldType = extractStringWithKeyFromArray($fieldName, $this->tableFieldTypes, 'string');
        $fieldType = Extract::singleton()->extractStringWithKey($fieldName, $this->tableFieldTypes,'string');
        if ($fieldName == $this->primaryKey) {
            $fieldType = 'number';
        }

        //	echo get_class($this) . " ". $fieldName . "<br/>";

        $v = $this->{ $fieldName };

        switch($fieldType){

            case "foreignKey":

                //	if (is_null($v))
                //		return $v;

                if ($v instanceof ORM && isset($this->tableFieldTypes[$fieldName]) && $this->tableFieldTypes[$fieldName] == 'foreignKey') {
                    return ($v->getPrimaryKey() * 1);
                }

            //otherwise, we fallthough to the number..
            //else if (is_numeric($v)){
            //	return new Zend_Db_Expr( ($this->{ $fieldName } *1) ); //turn it into a real number
            //}
            //else {
            //	throw new Exception("Odd ball data for fk: " . gettype($v) . ", " . gettype($this)	);
            //	die("not sure what we have here");
            //}
            //break;

            case "number":
                if (is_null($v))
                    return $v;

                return new \Zend_Db_Expr( ($v * 1) );//turn it into a real number
                break;

            case 'dateTime':

                if (is_null($v))
                    return null;

                if ($v instanceof \DateTime || $v instanceof Date || $v instanceof DateTime){
                    return $v->format("Y-m-d H:i:s");
                }

                return date("Y-m-d H:i", strtotime($v));
                break;

            case 'dateAsMySQLTimestamp':
                if (is_null($v))
                    return null;
                if ($v instanceof \DateTime){
                    return date("Y-m-d H:i", $v->getTimestamp());
                }

                return date("Y-m-d H:i", strtotime($v));

                break;

            case "boolean":
                $v = Extract::singleton()->extractBooleanWithKey('v', array('v'=>$v), false);
                //	$v = valueToBoolean($v, false);
                $r = new \Zend_Db_Expr($v ? "true" : "false");
                return $r;
                break;

            //case "file":
            //
            //
            //	if (is_string($v) && file_exists($v)) {
            //		return new Zend_Db_Expr( '' );
            //	} else {
            //		// if we are updatin.. we dont want to kill the data.. wiht null.. and there is no way to check if we are deleting the binary
            //		// data. so do nothing if we are not setting a new file..
            //		return $v;
            //	}
            //
            //break;

            case 'string':
            default:

                if (is_null($v))
                    return $v;

                return $v;
                break;
        }

    }

    /**
     * Gets an object with a given identifier
     * @param unknown_type $id
     * @param Zend_Db_Adapter_Abstract $zdb
     * @return  object
     */
    public static function getObjectWithId($id,Zend_Db_Adapter_Abstract $zdb){

        if (is_null($id)) return null;

        //print_r($id);
        $myClass = get_called_class();
        $o = new $myClass($zdb);

        if ($o->load($id))
            return $o;
        else
            return null;

    }

    /**
     *
     * Returns an array of objects that match the id's in the provided array
     * @param $idArray an array of identifiers
     * @param $zdb the database connection to use
     * @return array
     */
    public static function getObjectsWithIds(array $idArray, \Zend_Db_Adapter_Abstract $zdb){
        $objects=array();

        foreach($idArray as $id){
            $o = static::getObjectWithId($id,$zdb);
            if ($o){
                array_push($objects,$o);
            }
        }
        return $objects;
    }

    //array access implementation
    public function offsetExists($offset){
        return isset($this->{$offset});
    }

    public function offsetGet($offset){
        if (!$this->offsetExists($offset)){
            throw new \OutOfBoundsException();
        }
        return $this->{ $offset };
    }

    public function offsetSet($offset, $value){
        $this->{ $offset } = $value;
    }

    public function offsetUnset($offset){
        unset($this->{ $offset });
    }




    //Traversable/
    public function current (){
        if (is_null($this->_iteratorKeys)) $this->rewind();

        return $this->{ current($this->_iteratorKeys )};
    }

    public function key (){
        if (is_null($this->_iteratorKeys)) $this->rewind();
        return current($this->_iteratorKeys);
    }

    public function next (){

        if (is_null($this->_iteratorKeys)) $this->rewind();

        next($this->_iteratorKeys);
    }

    public function rewind (){
        $this->_iteratorKeys = array_merge($this->tableFields, array_keys($this->tableRelationships));
    }

    public function valid (){
        if (is_null($this->_iteratorKeys)) $this->rewind();

        return isset($this->{ current( $this->_iteratorKeys ) });
    }

}
