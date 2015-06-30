# ElasticSearchSync
## Supported model example

### Primary object and Nested object 
#### sql data
```
	key		|	field1	|	field2	|	nested1.field3	|	nested1.nested2.field4	|	nested1.nested2.field5
	1		|	val1	|	val2	|	nes1val3		|	nes1nes2val4			|	nes1nes2val5
```
#### serialized object (basic model)
```
{
	field1: val1
	field2: val2
	nested1: {
		field3: nes1val3
		nested2: {
			field4: nes1nes2val4
			field5: nes1nes2val5
		}
	}
}
```


### Array object into Primary object or Nested object (continuing with basic model) (array model)
#### sql data
```
	primary object key		|	key		|	field10	|	nested5.field11	|	nested5.nested6.field12
	1						|	2		|	val10	|	nes5val11		|	nes5nes6val12
```
#### serialized object
##### attributeName: array (inserted into primary object)
```
{
	field1: val1
	..
	array: [{
		key: 2
		field10: val10
		nested5: {
			field11: nes5val11
			nested6: {
				nes5nes6val12
			}
		}
	}]
}
```
##### attributeName: nested1.nested2.array (inserted into nested object)
```
{
	..
	nested1: {
		..
		nested2: {
			..
			array: [{
				key: 2
				field10: val10
				nested5: {
					field11: nes5val11
					nested6: {
						nes5nes6val12
					}
				}
			}]
		}
	}
}
```


### Array object into another array (continuing with array model)
#### sql data
```
	primary object key		|	array key	|	key		|	field13	|	nested7.field14
	1						|	2			|	3		|	val13	|	nes7val14
```
#### serialized object
##### attribute name: array.arrayInception
```
{
	field1: val1
	..
	array: [{
		key: 2
		field10: val10
		nested5: {
			field11: nes5val11
			nested6: {
				nes5nes6val12
			}
		}
		arrayInception: [{
			key: 3
			field13: val13
			nested7: {
				field14: nes7val14
			}
		}]
	}]
}
```
##### attribute name: array.nested5.nested6.arrayInception
```
{
	..
	nested1: {
		..
		nested2: {
			..
			array: [{
				key: 2
				field10: val10
				nested5: {
					field11: nes5val11
					nested6: {
						nes5nes6val12
						arrayInception: [{
							key: 3
							field13: val13
							nested7: {
								field14: nes7val14
							}
						}]
					}
				}
			}]
		}
	}
}
```


### Nested object built with Row paradigm (continuing with basic model)
#### sql data
```
	primary object key		|	label					|	value
	1						|	field6					|	val6
	1						|	nested3.field7			|	nes3val7
	1						|	nested3.nested4.field8	|	nes3nes4val8
	1						|	nested3.nested4.field9	|	nes3nes4val9
```
#### serialized object
##### attributeName: rowParadigm
```
{
	field1: val1
	..
	rowParadigm: {
		field6: val5
		nested3: {
			field7: nes3val7
			nested4: {
				field8: nes3nes4field8
				field9: nes3nes4field9
			}
		}
	}
}
```
##### attributeName: nested1.nested2.rowParadigm
```
{
	..
	nested1: {
		..
		nested2: {
			..
			rowParadigm: {
				field6: val5
				nested3: {
					field7: nes3val7
					nested4: {
						nes3nes4field8
						nes3nes4field9
					}
				}
			}
		}
	}
}
```


### Nested object built with Row paradigm into an existing array (continuing with array model)
#### sql data
```
	primary object key		|	array key	|	label					|	value
	1						|	2			|	field15					|	val15
	1						|	2			|	nested8.field16			|	nes8val14
```
#### serialized object
##### attributeName: array.rowParadigmInArray
```
{
	field1: val1
	..
	array: [{
		key: 2
		field10: val10
		nested5: {
			field11: nes5val11
			nested6: {
				nes5nes6val12
			}
		}
		rowParadigmInArray: {
			field15: val15
			nested8: {
				field16: nes8val14
			}
		}
	}]
}
```
##### attributeName: array.nested5.rowParadigmInArray
```
{
	..
	nested1: {
		..
		nested2: {
			..
			array: [{
				key: 2
				field10: val10
				nested5: {
					field11: nes5val11
					nested6: {
						..
					}
					rowParadigmInArray: {
						field15: val15
						nested8: {
							field16: nes8val14
						}
					}
				}
			}]
		}
	}
}
```
