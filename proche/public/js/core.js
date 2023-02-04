var search;
var search_csv;
var collapsed_visible_state={}
var expand_facets={};

var test_collapsed=function(id_ctrl)
{
	var is_visible= $(id_ctrl).css("display");
	console.log(is_visible);
	collapsed_visible_state[id_ctrl]=is_visible;	
}

var increase_facet_size=function(field, value)
{
	if(!(field in expand_facets ))
	{
		expand_facets[field]=0;
	}
	expand_facets[field]=expand_facets[field]+value;
	console.log(expand_facets);
	
}

var select2_generic=function(url, key, val, minlen)
{
	return select2_generic_full(url, key, val, minlen, true);
}

var select2_generic_full=function(url, key, val, minlen, include_pattern)
		{
			var global_pattern="";
			return {
						tags:true,
                        minimumInputLength: minlen,
                        ajax:
                        {
                            url:url,
                            dataType: 'json',
                            data: function(param)
                            {
                                global_pattern= param.term;
                                return {
									
                                    q: param.term,
                                    f: key
                                };
                            }
                            
                            
                        }
                    
                    }
		}

  function onElementInserted(containerSelector, elementSelector, callback) {

            var onMutationsObserved = function(mutations) {
				
                mutations.forEach(function(mutation) {
                    if (mutation.addedNodes.length) {
                        var elements = $(mutation.addedNodes).find(elementSelector);
                        for (var i = 0, len = elements.length; i < len; i++) {
                            callback(elements[i]);
                        }
                    }
                });
            };

            var target = $(containerSelector)[0];
            var config = { childList: true, subtree: true };
            var MutationObserver = window.MutationObserver || window.WebKitMutationObserver;
            var observer = new MutationObserver(onMutationsObserved);    
            observer.observe(target, config);

        }
		
		
		    //ftheeten 2018 09 18
    function onElementModified(containerSelector, elementSelector, callback) {

            var onMutationsObserved = function(mutations) {				
                mutations.forEach(function(mutation) {                  
                      
							 var elements = $(elementSelector);
								for (var i = 0, len = elements.length; i < len; i++) 
								{									
									callback(elements[i]);
								}
                });
            };

            var target = $(containerSelector)[0];
            var config = { childList: true, subtree: true };
            var MutationObserver = window.MutationObserver || window.WebKitMutationObserver;
            var observer = new MutationObserver(onMutationsObserved);    
            observer.observe(target, config);

        }
		
	var get_param=function( name, url ) 
	{
        if (!url) url = location.href;
        name = name.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
        var regexS = "[\\?&]"+name+"=([^&#]*)";
        var regex = new RegExp( regexS );
        var results = regex.exec( url );
        return results == null ? "" : results[1];
    }
	
	 var get_all_params=function(url)
    {       
		console.log(url);
        var regexS = /(?<=&|\?)([^=]*=[^&#]*)/;
        var regex = new RegExp( regexS,'g' );
        var results = url.match(regex);
        if(results==null)
        {
            return {};
        }
        else
        {
            returned={};
            for(i=0;i<results.length;i++)
            {
                var tmp=results[i];                
                var regexS2="([^=]+)=([^=]+)";
                var regex2 = new RegExp( regexS2 );
                var results2 = regex2.exec(tmp );                
                returned[results2[1]]=results2[2];
            }
            return returned;
        }   
    }
	
	var replace_param=function(url, param, value)
	{
		var get_params=get_all_params(url);
		var base_url=url.split("?");
		get_params[param]=value;
		var new_params=Array();
		for(key in get_params)
		{
			new_params.push(key+"="+get_params[key]);
		}
		return base_url[0]+"?"+new_params.join("&");
	}		