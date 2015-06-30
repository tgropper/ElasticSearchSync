# ElasticSearchSync
## Supported model

```
<primary object>
	<array object> [ /2 levels/
		<nested object> /N levels/
		<nested object built with row paradigm> /N levels/Do not support array within it yet/
		<array object> [ /Do not support any more levels of array within it yet/
			..
		]
	]
	<nested object> /N levels/
		<nested object built with row paradigm> /N levels/Do not support array within it yet/
		<array object> [ /2 levels/
			..
		]
	<nested object built with row paradigm> /N levels/Do not support array within it yet/
```
