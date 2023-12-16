var search;
var search_csv;
var collapsed_visible_state={}
var expand_facets={};
var mobile_expand_facet=false;
var carousel_size={};
var carousel_current={};

var test_collapsed=function(id_ctrl)
{
	var is_visible= $(id_ctrl).css("display");
	//console.log(is_visible);
	collapsed_visible_state[id_ctrl]=is_visible;	
}

var increase_facet_size=function(field, value)
{
	if(!(field in expand_facets ))
	{
		expand_facets[field]=0;
	}
	expand_facets[field]=expand_facets[field]+value;
	//console.log(expand_facets);
	
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
		//console.log(url);
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
	
	
	function get_all_json_keys(json_object, collect_all_keys, parent) 
	{
		////console.log(json_object);
		for (json_key in json_object) 
		{
			////console.log("=>"+json_key);
			complete_path=parent+"/"+json_key;
			if(! (complete_path in collect_all_keys))
			{
				collect_all_keys[complete_path]=[];
			}
			if (typeof(json_object[json_key]) === 'object' && !Array.isArray(json_object[json_key])) 
			{
				//ret_array.push(json_key);				
				
				collect_all_keys[complete_path].push(json_object[json_key]);
				get_all_json_keys(json_object[json_key], collect_all_keys, complete_path);
			} 
			else if (Array.isArray(json_object[json_key])) 
			{
				////console.log("array");
				if(json_object[json_key].length>0)
				{
					////console.log("go");
					//ret_array.push(json_key);
					
					//
					first_element = json_object[json_key][0];
					if(Array.isArray(first_element))
					{
						////console.log("nested array");
						for(var i=0;i<json_object[json_key].length; i++ )
						{
							get_all_json_keys(json_object[json_key][i],collect_all_keys, complete_path);
						}
					}
					else if (typeof(first_element) === 'object') 
					{
						////console.log("nested object");
						////console.log(first_element);
						get_all_json_keys(first_element, collect_all_keys, complete_path);
					}					
					else
					{
						//console.log("array value");
						for(var i=0;i<json_object[json_key].length; i++ )
						{
							collect_all_keys[complete_path].push(json_object[json_key][i]);
						}
						
					}
				}
			} 
			else 
			{			
				collect_all_keys[complete_path].push(json_object[json_key]);
			}
		}
		return collect_all_keys;
		//return ret_array
	}
	
	var get_thumbnail=function(url, size)
	{
		 url=url.replace("/full/full/", '/full/!'+size+','+size+'/');
		 return url;
		 
	}
	
	var add_thumbnail=function(url, size)
	{
		 url=url+"/full/!"+size+","+size+"/0/default.jpg";
		 return url;
		 
	}
	
	var handle_iiif_main=function(url, p_list_iiif, p_list_thumbs, p_list_attributions)
	{
		
		//console.log(url);
		var jqxhr = $.getJSON( url).done(function( data ) 
		{
			
			//console.log(data);
			if("sequences" in data)
			{
				//console.log("seq");
				//console.log(data['sequences']);
				for(const [key, value] of Object.entries(data['sequences']))				
				{
					if("canvases" in value)
					{
						//console.log("canvases");
					
						for (const [key1, value1] of Object.entries(value["canvases"])) 
						{
							if("images" in value1)
							{						
									
										for (const [key2, value2] of Object.entries(value1["images"])) 
										{
											if("resource" in value2)
											{
												img=value2["resource"]["@id"];
												//console.log(img);
												p_list_thumbs.push(get_thumbnail(img, thumb_size));
												if("service" in value2["resource"])
												{
													p_list_iiif.push( value2["resource"]["service"]["@id"]+'/info.json');
												}
												p_list_attributions.push("");
											}
										}
									
							}
						}
					}
					
				}
			}
			else
			{
				var collect_all_keys={};
				get_all_json_keys(data,collect_all_keys, "" );
				//console.log("collect=");
				//console.log(collect_all_keys);
				if("/items/items/items/body/service/id" in collect_all_keys)
				{
					var flag_attribution=false;
					if("/items/requiredStatement/value/en" in collect_all_keys)
					{
						flag_attribution=true;
					}
					//console.log("proceed service");
					for(var i=0; i<collect_all_keys["/items/items/items/body/service/id"].length; i++)
					{
						p_list_thumbs.push(add_thumbnail(collect_all_keys["/items/items/items/body/service/id"][i], thumb_size));
						p_list_iiif.push( collect_all_keys["/items/items/items/body/service/id"][i]+'/info.json');
						var attribution="";
						if(flag_attribution)
						{
								if(i<collect_all_keys["/items/requiredStatement/value/en"].length)
								{
									attribution=collect_all_keys["/items/requiredStatement/value/en"][i];
								}
						
						}
						p_list_attributions.push(attribution);
					}
				}
				
					
					
				
			}
			
			display_thumbs_main(p_list_thumbs, p_list_attributions);
			//console.log(list_iiif);
			init_ol();
		});
		
		$("#close_modal").click(
			function()
			{
				$('#modal_iiif').modal('hide');
			}
		)
	}
	
	var display_iiif_thumbs_carousel=function(list, id, ctrl)
	{
		var html_imgs=Array();
		var i=0;
		
		if(list.length<2)
		{
			$("#carousel-control-prev"+id.toString()).hide();
			$("#carousel-control-next"+id.toString()).hide();
		}
		
		while(i<list.length)
		{
			 var active="";
			 if(i==0)
			 {
				active=" active"
			 }
			 html_imgs.push('<div class="carousel-item'+active+'" id="frame_proche_img_'+id.toString()+'_'+i.toString()+'"><img class="d-block img-fluid" src="'+list[i]+'" ></div>');
			 
			i++;
		}
		$("#"+ctrl).append(html_imgs.join(''));
		
		carousel_size[id]=list.length;
		carousel_current[id]=0;
	}
	
	var next_carousel=function(id, ctrl)
	{
		var max_ci=carousel_size[id];
		var current=carousel_current[id];
		//console.log(current);
		//console.log(max_ci);
		if(current<max_ci-1)
		{
			$("#"+ctrl+current.toString()).removeClass("active");
			current++;
			$("#"+ctrl+current.toString()).addClass("active");
			
		}
		else
		{
			$("#"+ctrl+current.toString()).removeClass("active");
			current=0;
			$("#"+ctrl+current.toString()).addClass("active");
		}
		
		carousel_current[id]=current;
	}
	
	var previous_carousel=function(id, ctrl)
	{
		var max_ci=carousel_size[id];
		var current=carousel_current[id];
		if(current>0)
		{
			$("#"+ctrl+current.toString()).removeClass("active");
			current--;
			$("#"+ctrl+current.toString()).addClass("active");
			
		}
		else
		{
			$("#"+ctrl+current.toString()).removeClass("active");
			current=max_ci-1;
			$("#"+ctrl+current.toString()).addClass("active");
		}
		carousel_current[id]=current;
	}
	
	
	
	
	
	
	
	var handle_iiif_thumbs=function(url,id, ctrl, thumb_size, p_list_thumbs, p_list_attributions)
	{
		
		////console.log(url);
		var jqxhr = $.getJSON( url).done(function( data ) 
		{
			var list_thumbs=Array();
			////console.log(data);
			if("sequences" in data)
			{
				////console.log("seq");
				////console.log(data['sequences']);
				for(const [key, value] of Object.entries(data['sequences']))				
				{
					if("canvases" in value)
					{
						////console.log("canvases");
					
						for (const [key1, value1] of Object.entries(value["canvases"])) 
						{
							if("images" in value1)
							{						
									
										for (const [key2, value2] of Object.entries(value1["images"])) 
										{
											if("resource" in value2)
											{
												img=value2["resource"]["@id"];
												////console.log(img);
												p_list_thumbs.push(add_thumbnail(img, thumb_size));
											}
										}
									
							}
						}
					}
					
				}
			}
			else
			{
				var collect_all_keys={};
				get_all_json_keys(data,collect_all_keys, "" );
				//console.log("collect=");
				//console.log(collect_all_keys);
				if("/items/items/items/body/service/id" in collect_all_keys)
				{
					//console.log("proceed service");
					for(var i=0; i<collect_all_keys["/items/items/items/body/service/id"].length; i++)
					{
						p_list_thumbs.push(add_thumbnail(collect_all_keys["/items/items/items/body/service/id"][i], thumb_size));
					}
				}
				
				if("/items/requiredStatement/label/en" in collect_all_keys)
				{
					//console.log("proceed service");
					for(var i=0; i<collect_all_keys["/items/requiredStatement/value/en"].length; i++)
					{
						p_list_attributions.push(collect_all_keys["/items/requiredStatement/value/en"][i]);
					}
				}
				if(p_list_attributions.length>0)
				{
					
					$("#image_attribution_"+id.toString()).html(p_list_attributions.join(","));
				}
				if("/items/requiredStatement/value/en" in collect_all_keys)
				{
					//console.log("proceed service");
					for(var i=0; i<collect_all_keys["/items/requiredStatement/value/en"].length; i++)
					{
						p_list_attributions.push(collect_all_keys["/items/requiredStatement/value/en"][i]);
					}
				}
				if(p_list_attributions.length>0)
				{
					
					$("#image_attribution_"+id.toString()).html(p_list_attributions[0]);
				}
			}
			
			display_iiif_thumbs_carousel(p_list_thumbs, id, ctrl);
		});
	}


	