<?php
// src/Controller/ProcheController.php
namespace App\Controller;

use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\StreamedResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\Routing\Annotation\Route;
use Solarium\Client;
//use Solarium\Core\Client\Adapter\Curl;
//use App\Service\SiteUpdateManager;
//use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\HttpFoundation\JsonResponse;

use Symfony\Component\Translation\LocaleSwitcher;
use Symfony\Contracts\Translation\TranslatorInterface;

use Symfony\Component\HttpFoundation\Cookie;
use Symfony\Contracts\HttpClient\HttpClientInterface;
//use Symfony\Component\Messenger\MessageBusInterface;
//use App\Message\ProcheCsv;

class ProcheController extends AbstractController
{


	protected $default_lang="fr";
    private $client;
	private $page_size=10;

	
	private $localeSwitcher;
	//private $bus;
	private $facet_size=10;
	//private $session;

	//private $list_included_fields_csv=["id","object_number", "sort_number",	"title"	, "dimensions",	"date_of_acquisition","acquisition_method",	"creation_date","culture","objtitle_legacy","_version_",	"score"];
	
	private $http_client;

	
	
	
	/** @var \Solarium\Client */
   public function __construct(\Solarium\Client $client,  LocaleSwitcher $localeSwitcher, HttpClientInterface $http_client ) {
	   
       $this->client = $client;
	   $this->localeSwitcher=$localeSwitcher;
	   //$this->bus=$bus;
	   $this->http_client=$http_client;
	  
    }
	
	
	protected function remove_punctuation($val)
	{
		$val=str_replace("-"," ",$val);
		$val=str_replace(":"," ",$val);
		$val=str_replace(","," ",$val);
		$val=str_replace(";"," ",$val);
		$val=str_replace("!"," ",$val);
		$val=str_replace("?"," ",$val);
		//$val=str_replace("."," ",$val);
		$val=str_replace(". "," ",$val);
		$val=str_replace(" ."," ",$val);
		$val=str_replace("("," ",$val);
		$val=str_replace(")"," ",$val);
		//$val=str_replace("'"," ",$val);
		$val=str_replace('"'," ",$val);
		$val = preg_replace('/\s+/', ' ', $val);
		return $val;
	}
	
	protected function escapeSolr($helper, $input, $fuzzy=true)
	{
		
		if($fuzzy)
		{
			$tmp=trim($helper->escapePhrase($input),'"');
			$tmp=$this->remove_punctuation($tmp);
			return '"*'.$tmp.'*"';
		}
		else
		{
			$input=trim($helper->escapePhrase($input),'"');
			$input=$this->remove_punctuation($input);
			return  $input;
		}
	}
	
	protected function escapeSolrArray($helper, $vals, $fuzzy=true)
	{
		$returned=[];
		foreach($vals as $val)
		{
			if($fuzzy)
			{
				$val=$this->remove_punctuation($val);
			}
			if($fuzzy)
			{
				
				$returned[]='"*'.trim(trim($helper->escapePhrase( $val),'"')).'*"';
				
			}
			else
			{
				$returned[]=trim($helper->escapePhrase( $val));
			}
		}
		
		return  $returned;
	}
	
	protected function set_lang_cookie($lang)
	{
		$response = new Response();
		$response->headers->setCookie( Cookie::create('proche_locale', $lang));
		$response->sendHeaders();
		
		
	}
	
	protected function get_lang_cookie($request)
	{
		$lang=$request->cookies->get('proche_locale',$this->default_lang);
		return $lang;
		
	}
	
	#[Route('/', name:"home")]
	public function home(Request $request): Response
    {
		//print_r($this->getParameter('endpoints',"solr"));
		$lang=$this->get_lang_cookie($request);
		$this->localeSwitcher->setLocale($this->default_lang);
		$this->set_lang_cookie($this->default_lang);
		return $this->render('extra_pages/pageabout.html.twig',[]);
	}
	
	#[Route('/{locale}', name:"homelang", requirements: ['locale' => '(en|fr|nl)'])]
	public function homelang(Request $request, $locale="fr"): Response
    {
		$session=$request->getSession();
		$this->localeSwitcher->setLocale($locale);
		$session->set('current_locale', $locale);
		$this->set_lang_cookie($locale);
		return $this->render('extra_pages/pageabout.html.twig',[]);
	}
	
	
	#[Route('/simplesearch', name: 'simplesearch')]	
    public function simplesearch(Request $request): Response
    {		
		$lang=$this->get_lang_cookie($request);
		$this->localeSwitcher->setLocale($request->getSession()->get('current_locale',$lang));
		$dyna_field_free=$this->getParameter('free_text_search_field',[]);		
		$dyna_field_details=$this->getParameter('detailed_search_fields',[]);
		$cookie_disclaimer=$this->get_disclaimer_cookie($request);
        return $this->render('home.html.twig',["dyna_field_free"=>$dyna_field_free, "dyna_field_details"=>$dyna_field_details, "cookie_accepted"=>$cookie_disclaimer]);
    }
	
	#[Route('/simplesearch/{locale}', name: 'simplesearchlang', requirements: ['locale' => '(en|fr|nl)'])]	
    public function simplesearchlang(Request $request, $locale="fr"): Response
    {
		$cookie_locale=$request->cookies->get('proche_locale',"");
		
		$session=$request->getSession();
		$this->localeSwitcher->setLocale($locale);
		$session->set('current_locale', $locale);
		$this->set_lang_cookie($locale);
		$dyna_field_free=$this->getParameter('free_text_search_field',[]);
		$dyna_field_details=$this->getParameter('detailed_search_fields',[]);
		$cookie_disclaimer=$this->get_disclaimer_cookie($request);
        return $this->render('home.html.twig',["dyna_field_free"=>$dyna_field_free, "dyna_field_details"=>$dyna_field_details, "cookie_accepted"=>$cookie_disclaimer]);
    }
	
	#[Route('/detailed_searches', name: 'detailed_search')]	
    public function home_detail(Request $request): Response
    {
		
		$dyna_field_details=$this->getParameter('detailed_search_fields',[]);
		$dyna_field_free=$this->getParameter('free_text_search_field',[]);
		
		$this->localeSwitcher->setLocale($request->getSession()->get('current_locale','fr'));
		$response = new Response();
		$cookie_disclaimer=$this->get_disclaimer_cookie($request);
        return $this->render('home_details.html.twig',["dyna_field_free"=>$dyna_field_free, "dyna_field_details"=>$dyna_field_details, "cookie_accepted"=>$cookie_disclaimer]);
    }
	
	
	#[Route('/detail', name: 'detail')]
	public function detail(Request $request): Response
    {
		
		$this->localeSwitcher->setLocale($request->getSession()->get('current_locale','fr'));		
		$this->set_lang_cookie( $request->getSession()->get('current_locale','fr'));
		$client=$this->client;
		$id=$request->get("q","");
		//if(is_numeric($id))
		//{
			$detail_main_title_field=$this->getParameter("detail_main_title_field", "id");
			$detail_sub_title_field=$this->getParameter("detail_sub_title_field", "id");
			$detail_fields=$this->getParameter("detail_fields",[]);
			if(strlen(trim($id))>0)
			{
				
				$query = $client->createSelect();
				$query->setQuery($this->getParameter('link_field',"id").":".$id);
				$rs_tmp= $client->select($query);
				foreach ($rs_tmp as $document) 
				{
					$doc=[];
					foreach ($document as $field => $value) 
					{
						$doc[$field]=$value;
					}
					$rs[]=$doc;
					
				}
				if(count($rs)>=1)
				{
					$detail=$rs[0];
					$cookie_disclaimer=$this->get_disclaimer_cookie($request);
					return $this->render('detail.html.twig',[
						"doc"=>$detail, 
						"detail_main_title_field"=> $detail_main_title_field, 
						"detail_sub_title_field"=> $detail_sub_title_field, 
						"detail_fields"=>$detail_fields, 
						"cookie_accepted"=>$cookie_disclaimer]);
				}
			}
		//}
		
		return $this->render('noresults.html.twig');
	}
	
	protected function strip_accent($str) 
	{
		$test= strtr(utf8_decode($str), utf8_decode('àáâãäçèéêëìíîïñòóôõöùúûüýÿÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝ'), 'aaaaaceeeeiiiinooooouuuuyyAAAAACEEEEIIIINOOOOOUUUUY');
		
		return $test;
	}
	
	protected function refine_autocomplete($results, $term)
	{
		$control1=Array();
		$control2=Array();
		foreach($results as $tmp0)
		{
			$control1[]=$tmp0["text"];
		}

		$tmp_result=Array();
		//$term=strtolower($term);
		
		foreach($control1 as $r)
		{
			
			$tmp2=explode(" ", $r);
			
			

			foreach($tmp2 as $term2)
			{
				
				if(mb_stripos( $term2, $term)!==false)
				{
					
					if(! in_array($term2,$control2 ) && ! in_array($term2,$control1 ))
					{
						
						$tmp_result[]=["cpt"=>1, "text"=>$term2];
						$control2[]=$term2;
					}
				}
			}
		}

		if(count($tmp_result)>0)
		{
			$results=array_merge($tmp_result, $results);
		}
		return $results;
	}
	
	protected function logic_autocomplete($field, $value)
	{
		$returned=[];
		$client=$this->client;
		$str="";
		  
		  
		   //$field=urldecode( $field);
		   $fs=explode("|", $field);
		   if(count( $fs)==1)
		   {
			   if(strlen(trim($field))>0&&strlen(trim($value))>0)
			   {
				   
				   
				   
					
						// create a facet field instance and set options
						$resp=[];
						 $list=preg_split("/\s+/", $value);
						 
						 foreach($list as $keyword)
						 {
							if(strlen($keyword)>1)
							{
								$query = $client->createSelect();
								$query->setStart(0)->setRows(0);
								$facetSet = $query->getFacetSet();
								$facetSet->createFacetField('sfitems')->setField($field)->setContains($keyword)->setContainsIgnoreCase(true)->setSort('asc');
								$query->setQuery('*:*');
								$returned_tmp = $client->select($query);
								$facets =$returned_tmp->getFacetSet()->getFacet('sfitems');
								foreach($facets as $vf => $count) 
								{
									$tmp["original"]=$vf;
									$tmp["non_accent"]=$this->strip_accent($vf);
									$resp[$vf]=$tmp;
								}
							
							}
							
						  
						}
						$list=  array_map(array($this, 'strip_accent'), $list);
						
						//$resp=array_unique($resp);
						
						//$resp_accent=  array_map(array($this, 'strip_accent'), $resp);
						
						//sort($resp);
						
						$sort=Array();
						
						if($list>0)
						{
							$i_resp=0;
							foreach($resp as $key=>$tmp_cluster)
							{
								
								$tmp_v= $tmp_cluster["non_accent"];
								$tmp_v_original= $tmp_cluster["original"];
								$cpt=0;
								foreach($list as $keyword)
								{
									//print("/(.*|^)".$keyword."(.*|$)/i");
									//print("\n");
									$test=preg_match("/(^|\s)".$keyword."(\s|$)/i", $tmp_v);
									//$test=preg_match("/(.*|^)".$keyword."(.*|$)/i", $tmp_v);
									if($test!==false)
									{
										if($test>0)
										{
											$cpt=$cpt+2;
										}
										else
										{
											$test=preg_match("/(.*|\s)".$keyword."(.*|$)/i", $tmp_v);
											if($test!==false)
											{
												if($test>0)
												{
													$cpt=$cpt+1;
												}
											}
										}
									}
									
								}
								//$sort[$tmp_v]=$cpt;
								$sort[$tmp_v_original]["cpt"]=$cpt;
								$sort[$tmp_v_original]["text"]=$tmp_v_original;
							}	
							$i_resp++;	
						}
						
						//arsort($sort);
						
						 usort($sort,
								 function ($a, $b)
								 {
									 /*if (strlen($a['text']) == strlen($b['text'])) 
									 {
										return 0;
									}
									else
									{
										if($a['cpt']==$b['cpt'])
										{
											return (strlen($a['text']) < strlen($b['text'])) ? -1 : 1;
										}
										elseif($a['cpt']<$b['cpt'])
										{
											return 1;
										}
										elseif($a['cpt']>$b['cpt'])
										{
											return -1;
										}
									}*/
									
									if($a['cpt']==$b['cpt'])
									{
											if (strlen($a['text']) == strlen($b['text'])) 
											{
												return 0;
											}
											elseif(strlen($a['text']) < strlen($b['text']))
   											{
												return -1;
											}
											else
											{
												return 1;
											}
									}
									elseif($a['cpt']<$b['cpt'])
									{
											return 10;
									}
									elseif($a['cpt']>$b['cpt'])
									{
											return -10;
									}
									//return (strlen($a['text']) < strlen($b['text'])) ? -1 : 1;
								 }
							 );
						$sort=$this->refine_autocomplete($sort, $value);
						$resp2=Array();
						foreach($sort as $tmp_v=>$word)
						{
							$resp2[]=["id"=>$word["text"], "text"=>$word["text"]];
						}
						//array_unshift($resp2, ["id"=>$value, "text"=>$value]);
						$returned=$resp2;
						//$returned=[
						//			"results"=>$resp2
						//		];
					
			   }
		   }
		   elseif(count( $fs)>1)
		   {
			  $query = $client->createSelect();
			  $query->setStart(0)->setRows(0);
			  $facetSet = $query->getFacetSet();
			  $i=1;
			  foreach($fs as $tmp_field)
			  {
				 $newfield='sfitems'.$i;
				
				 $facetSet->createFacetField($newfield)->setField($tmp_field)->setContains($value)->setContainsIgnoreCase(true)->setSort('asc');
				 $query->setQuery('*:*');
				 $i++; 
			  }
			   
			  $returned_tmp = $client->select($query);
			  $resp=[];
			  for($j=1;$j<$i;$j++)
			  {
				   $newfield='sfitems'.$j;
				 
				   $facets =$returned_tmp->getFacetSet()->getFacet($newfield);
				   foreach($facets as $vf => $count) 
				   {
						$resp[]=$vf;
				   }
			  }
			  array_unique($resp);
			  sort($resp);
			  $resp2=[];
			  foreach($resp as $tmp_v)
			  {
				    $resp2[]=["id"=>$tmp_v, "text"=>$tmp_v];
			  }
			 
			/*$returned=[
						"results"=>$resp2
					];*/
			   $returned=$resp2;
			 
		   }
		   return $returned;
	}
	
	protected function logic_autocomplete_old($field, $value)
	{
		$returned=[];
		$client=$this->client;
		$str="";
		  
		  
		   //$field=urldecode( $field);
		   $fs=explode("|", $field);
		   if(count( $fs)==1)
		   {
			   if(strlen(trim($field))>0&&strlen(trim($value))>0)
			   {
				   $query = $client->createSelect();
				   $query->setStart(0)->setRows(0);
				   
				   $facetSet = $query->getFacetSet();
					
						// create a facet field instance and set options
						print($value);
						print(")))");
						$facetSet->createFacetField('sfitems')->setField($field)->setContains($value)->setContainsIgnoreCase(true)->setSort('asc');
						$query->setQuery('*:*');
						$returned_tmp = $client->select($query);
						$facets =$returned_tmp->getFacetSet()->getFacet('sfitems');
						$resp=[];
						
						foreach($facets as $vf => $count) 
						{
							$resp[]=$vf;
						}
						$resp=array_unique($resp);
						sort($resp);
						$resp2=[];
						foreach($resp as $tmp_v)
						{
							$resp2[]=["id"=>$tmp_v, "text"=>$tmp_v];
						}
						//array_unshift($resp2, ["id"=>$value, "text"=>$value]);
						$returned=$resp2;
						//$returned=[
						//			"results"=>$resp2
						//		];
					
			   }
		   }
		   elseif(count( $fs)>1)
		   {
			  $query = $client->createSelect();
			  $query->setStart(0)->setRows(0);
			  $facetSet = $query->getFacetSet();
			  $i=1;
			  foreach($fs as $tmp_field)
			  {
				 $newfield='sfitems'.$i;
				
				 $facetSet->createFacetField($newfield)->setField($tmp_field)->setContains($value)->setContainsIgnoreCase(true)->setSort('asc');
				 $query->setQuery('*:*');
				 $i++; 
			  }
			   
			  $returned_tmp = $client->select($query);
			  $resp=[];
			  for($j=1;$j<$i;$j++)
			  {
				   $newfield='sfitems'.$j;
				 
				   $facets =$returned_tmp->getFacetSet()->getFacet($newfield);
				   foreach($facets as $vf => $count) 
				   {
						$resp[]=$vf;
				   }
			  }
			  array_unique($resp);
			  sort($resp);
			  $resp2=[];
			  foreach($resp as $tmp_v)
			  {
				    $resp2[]=["id"=>$tmp_v, "text"=>$tmp_v];
			  }
			 
			/*$returned=[
						"results"=>$resp2
					];*/
			   $returned=$resp2;
			 
		   }
		   return $returned;
	}
	
	#[Route('/terms', name: 'terms')]
	public function terms(Request $request): JsonResponse
    {
		$field=$request->get("f","");
		$value=$request->get("q","");
		
		 $response=$this->logic_autocomplete($field, $value);
		 $list=preg_split("/\s+/", $value);
		 if(count($list)>1)
		 {
			
			$mode_append_term=false;
		 }
		 else
		 {
			
			$mode_append_term=true;
		 }
		
		 //$list=[];
		  if(count($response)==0)
		 {
			 
			 $settings_autocomplete=$this->getParameter('autocomplete_settings',[]);
			
			 $list_excluded_terms=$settings_autocomplete["excluded_terms"];
			 array_walk($list_excluded_terms, function(&$value)
			{
			  $value = strtolower($value);
			});
			 //$list=preg_split("/\s+/", $value);
			// $mode_append_term=false;
			$tmp_array=[];
			
					 
				if(!in_array(strtolower($value),  $list_excluded_terms))
				{
					
				 $tmp_response=$this->logic_autocomplete($field, $value);
				
				 if(count($tmp_response)>0)
				 {
					 if(!array_key_exists(count($tmp_response),$tmp_array))
					 {
						 $tmp_array[count($tmp_response)]=Array();
					 }
					 $tmp_array[count($tmp_response)][]=$tmp_response;
				  }
						 
				}
					 
			
			if(count( $tmp_array)>0)
			{
				ksort( $tmp_array);
				
				$response=Array();
				foreach ($tmp_array as $arrval) 
				{
				 foreach($arrval as $arrval2)
				 {
					foreach($arrval2 as $arrval3)
					{
						$response[] = $arrval3;
					}
				 }
			    }
			}
			 /*if(count($list)>0)
			 {
				 
				 $mode_append_term=false;
				 $tmp_array=[];
				 foreach($list as $sub_term)
				 {
					 
					 if(!in_array(strtolower($sub_term),  $list_excluded_terms))
					 {
						
						 $tmp_response=$this->logic_autocomplete($field, $sub_term);
						
						 if(count($tmp_response)>0)
						 {
							 if(!array_key_exists(count($tmp_response),$tmp_array))
							 {
								 $tmp_array[count($tmp_response)]=Array();
							 }
							 $tmp_array[count($tmp_response)][]=$tmp_response;
						 }
						 
					 }
					 
				 }
				 if(count( $tmp_array)>0)
				 {
					 ksort( $tmp_array);
					
					 $response=Array();
					 foreach ($tmp_array as $arrval) 
					 {
						 foreach($arrval as $arrval2)
						 {
							foreach($arrval2 as $arrval3)
							{
								$response[] = $arrval3;
							}
						 }
					}
				 }
			 }*/

		 }
		 
		 if($mode_append_term )
		 {
		   array_unshift($response, ["id"=>$value, "text"=>$value]);
		 }
		 
		 $resp_a=[];
		 /*if(count($list)>0)
		 { 
			
			$i=0;
			foreach($response as $resp)
			{
				
				$flag=true;
				foreach($list as $l_v)
				{
					if(mb_strpos(mb_strtolower($resp["text"]),mb_strtolower($l_v) )===false)
					{
						$flag=false;
						break;
					}
				}
				if($flag)
				{
					$resp_a=[$resp];
					unset($response[$i]);
				}
				$i++;
			}
		 }*/
		 /*usort($response,
			 function ($a, $b)
			 {
				 if (strlen($a['text']) == strlen($b['text'])) 
				 {
					return 0;
				}
				return (strlen($a['text']) < strlen($b['text'])) ? -1 : 1;
			 }
		 );*/
		 if(count($resp_a)>0)
		 {
			$response = array_merge($resp_a, $response);
		 }
		 
		 $returned=[
								"results"=>$response
							];
		
		 return $this->json($returned); 	
	}
	
	protected function createFacet($facet_set, $name_facet, $name_field, $limit=100000)
	{
		$facet_set->createFacetField($name_facet)->setField($name_field)->setMinCount(1)->setSort('desc')->setLimit($limit);
	}
	
	protected function returnSolrFacet($facet_set, $name_facet, $name_callback, $list_facet_fields, $nb_results, $limit=10)
	{
		
		$facets_title =$facet_set->getFacet($name_facet);
		$results_tmp=[];
		$results=[];
		$i=0;
	
		$test_check=false;
		$to_check=[];
		if(array_key_exists($name_callback,$list_facet_fields))
		{
			$test_check=true;
			
			$to_check=$list_facet_fields[$name_callback];
			$to_check=array_map(
				function($x)
				{
					return trim(trim($x,'"'),"'");
				}
				, $to_check);
			
			
			
		}
	
		foreach($facets_title as $vf => $count)
		{
			$results_tmp[$vf]=$count;
		}	
		$nb_entries=count($results_tmp);
		
	    arsort($results_tmp);
		
		$results_tmp=array_slice($results_tmp, 0, $limit, true);
		
		$inter_facets=array_intersect(array_keys($results_tmp) ,$to_check );
		$force_check=[];
		if(count($inter_facets)==0)
		{
			foreach($to_check as $val)
			{
				$results_tmp[$val]=$nb_results;
				$force_check[]=$val;
			}
		}
		$force_check=array_merge($force_check,$inter_facets );
		foreach($results_tmp as $vf => $count) 
		{
			
			if($count>0)
			{
				$results[$vf]=[];
				$results[$vf]["value"]=$count;
				//$results[$vf]["nb_entries"]=$nb_entries;
				if(in_array($vf,$force_check))
				{
					$results[$vf]["checked"]=true;
				}
				else
				{
					$results[$vf]["checked"]=false;
				}
			}
			
			$i++;	
		}
		
		$returned=Array();
		$returned["count"]=$nb_entries;
		$returned["results"]=$results;
		return $returned;
	}
	
	public function test_and_or($field, $request, $http_prefix)
	{
		if(strtolower($request->get($http_prefix.$field,"false"))=="true")
		{
			return " AND ";
		}
		else
		{
			return " OR ";
		}
	}
	
	#[Route('/main_search', name: 'main_search')]
	public function main_search(Request $request): Response
    {
		$this->localeSwitcher->setLocale($request->getSession()->get('current_locale','fr'));
		$this->set_lang_cookie( $request->getSession()->get('current_locale','fr'));
		$client=$this->client;
		
		

		$query = $client->createSelect();
		$helper = $query->getHelper();
		
		$nb_result=0;
		$size_csv=100;
		$rs=[];
		$current_page=$request->get("current_page",1);
        $page_size=$request->get("page_size",$this->page_size);
		$facet_filters=$request->get("facet_filters",[]);
		
		$display_facets=$request->get("display_facets",[]);
		$expand_facets=$request->get("expand_facets",[]);
		
		$with_images=$request->get('with_images',"false");
		
		
		$sort_field=$this->getParameter('sort_field',"id");
		$dyna_field_free=$this->getParameter('free_text_search_field',[]);
		$dyna_field_details=$this->getParameter('detailed_search_fields',[]);
		$dyna_field_facets=$this->getParameter('facet_fields',[]);
		
		
		$list_included_fields_csv=$this->getParameter('csv_fields',[]);
		
		
		
		$tech_fields_facets=$dyna_field_facets["fields"];
		$label_facets=$dyna_field_facets["labels"];
		$call_back_facets=$dyna_field_facets["filter_callback"];
		$i=0;
		$facet_labels=[];
		$facet_callbacks=[];
		foreach($tech_fields_facets as $tech_field)
		{
			$facet_labels[$tech_field]=$label_facets[$i];
			$facet_callbacks[$tech_field]=$call_back_facets[$i];
			$i++;
		}
		$title_field=$this->getParameter('title_field',"id");
		$link_field=$this->getParameter('link_field',"id");
		$result_fields=$this->getParameter('result_fields',[]);	
			
		
		$csv=false;
		if(strtolower($request->get("csv","false"))=="true")
		{
			$csv=true;
		}
		
		$offset=(((int)$current_page)-1)* (int)$page_size;
		$pagination=Array();
		
		
		$free_search=[];
		if(array_key_exists("field",$dyna_field_free)&&array_key_exists("label", $dyna_field_free))
		{
			$free_search=$this->escapeSolrArray($helper,$request->get("free_search",[]),false);
			
			
		}
		
		
		$list_detailed_fields=[];
		$params_and=[];
		if(array_key_exists("fields",$dyna_field_details ))
		{
			$dyna_fields=$dyna_field_details["fields"];
			$dyna_fields_matching=$dyna_field_details["matching"];
			$dyna_fields_types=$dyna_field_details["types"];
			$i=0;
			foreach($dyna_fields as $field)
			{
				if($dyna_fields_types[$i]=="date")
				{
					$date_begin=$request->get("search_".$field."_from","");
					$date_end=$request->get("search_".$field."_to","");
					if(strlen(trim($date_begin))>0&&strlen(trim($date_end))>0)
					{
					
						if($date_begin<=$date_end)
						{
							$params_and[]=$field.":[".$date_begin." TO ".$date_end."]";
						}
						else
						{
							$params_and[]=$field.":[".$date_end." TO ".$date_begin."]";
						}
					}
				}
				elseif($dyna_fields_matching[$i]=="exact")
				{
					$list_detailed_fields[$field]=$this->escapeSolrArray($helper,$request->get("search_".$field,[]), false);
				}
				else
				{
					$list_detailed_fields[$field]=$this->escapeSolrArray($helper,$request->get("search_".$field,[]));
				}
				$i++;
			}
		}	

		$list_facet_fields=[];
		foreach($facet_filters as $field=>$vals)
		{			
			$n_values=$vals["values"];
			$list_facet_fields[$field]=$this->escapeSolrArray($helper,$n_values, false);
			
		}
			
		
		
		$query_build="*:*";
		if($csv)
		{
			$query->setStart(0)->setRows(0);
		}
		else
		{
			$query->setStart($offset)->setRows($page_size);
		}
		$go=true;
		
		if(array_key_exists("field",$dyna_field_free )&&array_key_exists("label", $dyna_field_free))
		{
			
			if(count($free_search)>0)
			{		
				$params_and[]="(". $this->getParameter('free_text_search_field')["field"].": (".implode(" OR ", $free_search)."))";
				
			}
		}
		
		//loop detailed criterias
		foreach($list_detailed_fields as $field=>$filter)
		{
			
			if(count($filter)>0)
			{
				$params_and[]="(".$field.": (".implode($this->test_and_or($field, $request, "chkand_search_"),$filter)."))";
			}
		}
		
		foreach($list_facet_fields as $field=>$filter)
		{
			
			if(count($filter)>0)
			{
				$params_and[]="(".$field.": (".implode($this->test_and_or($field, $request, "chk_facet_and_search_"),$filter)."))";
			}
		}
		
		if(strtolower($with_images)=="true")
		{
			$params_and[]="(iiif_endpoint: [* TO *])";
		}
		
		if(count($params_and)>0)
		{
			$query_build=implode(" AND ", $params_and);
			$query->setQuery($query_build);
			
		}
		else
		{
				//get all
		}
		if(count($params_and)>0||$go)
		{
			$query->addSort($sort_field, $query::SORT_ASC);
			
			//facets
			if(count($dyna_field_facets)>0)
			{
				$facetSet = $query->getFacetSet();
				$facet_fields=$dyna_field_facets["fields"];
				foreach($facet_fields as $facet)
				{
					$this->createFacet($facetSet, "facet_".$facet, $facet);					
				}				
			}
			$query->setQueryDefaultOperator("AND");
			$rs_tmp= $client->select($query);	
			$nb_result=$rs_tmp->getNumFound();
			//debug
			//$base_url=$client->getEndPoint()->getCoreBaseUri();
			//$query_url=$base_url."select?q.op=AND&q=".$query_build."&rows=1000000&useParams=&wt=json";
			//print($query_url);
			if($csv)
			{
				
				$base_url=$client->getEndPoint()->getCoreBaseUri();
				$list_fields=implode(",",$list_included_fields_csv);
				$query_url=$base_url."select?fl=".$list_fields."&q.op=AND&q=".$query_build."&rows=1000000&useParams=&wt=csv";
				$response = $this->http_client->request(
					'GET',
						$query_url
					);
				$statusCode = $response->getStatusCode();
				
				// $statusCode = 200
				$contentType = $response->getHeaders()['content-type'][0];
				// $contentType = 'application/json'
				$content = $response->getContent();
				$response = new StreamedResponse();
				$response->setCallback(
					function () use ($content) 
					{
						ob_start();
						$handle = fopen('php://output', 'r+');
						fwrite($handle, $content);						
						fclose($handle);
						ob_flush();
					}
				);
				$response->headers->set('Content-Type', 'text/csv');       
				$response->headers->set('Content-Disposition', 'attachment; filename="dump.csv"');
				
				return $response;

			
			}
			else
			{
								
				$facets_twig=[];
				//$nb_result=$rs_tmp->getNumFound();
				
				if(count($dyna_field_facets)>0)
				{
					$facet_fields=$dyna_field_facets["fields"];
					$i=0;
					foreach($facet_fields as $facet)
					{
						
						if(array_key_exists($facet_callbacks[$facet], $expand_facets))
						{
							$facet_size=$expand_facets[$facet_callbacks[$facet]];
						}
						else
						{
							$facet_size=$this->facet_size;
						}
						$facet_tmp=$this->returnSolrFacet($rs_tmp->getFacetSet(), "facet_".$facet, $facet_callbacks[$facet], $list_facet_fields, $nb_result,$facet_size);
						$facets_twig[$i]["facet"]=$facet_tmp["results"];
						$facets_twig[$i]["facet_count"]=$facet_tmp["count"];
						$facets_twig[$i]["label"]=$facet_labels[$facet];
						$facets_twig[$i]["callback"]=$facet_callbacks[$facet];
						$facets_twig[$i]["facet_size"]=$this->facet_size;
						
						
						$and_checked=$request->get("chk_facet_and_search_".$facet_callbacks[$facet],"FALSE");
						$facets_twig[$i]["boolean_and_checked"]=$and_checked;
						
						$i++;
					}
				}
				
				$i=0;
				
				foreach ($rs_tmp as $document) 
				{
					$doc=[];
					foreach ($document as $field => $value) 
					{
						$doc[$field]=$value;
					}
					$rs[]=$doc;
					$i++;
				}
				$current_page=min($current_page, ceil($nb_result / $page_size));
				$current_page=max($current_page,1);
				$pagination = array(
					'page' => $current_page,
					'route' => 'search_main',
					'pages_count' => ceil($nb_result / $page_size)
				);
				
				if($nb_result>0)
				{
					return $this->render('results.html.twig',["results"=>$rs, "nb_result"=>$nb_result, "page_size"=>$page_size, "pagination"=>$pagination,
					'page' => $current_page,
					'dyna_field_facets'=>$facets_twig,
					"display_facets"=>$display_facets,
					"title_field"=>$title_field,
					"link_field"=>$link_field,
					"result_fields"=>$result_fields ]);
				
				}
				else
				{
					return $this->render('noresults.html.twig');
				}
			}
		}
	
		
		return $this->render('noresults.html.twig');
	}
	
	#[Route('/extrapage/{id}', name: 'extrapage')]	
	public function extraPage($id, Request $request): Response
	{
		$lang=$this->get_lang_cookie($request);
		$this->localeSwitcher->setLocale($this->default_lang);
		$cookie_disclaimer=$this->get_disclaimer_cookie($request);
		return $this->render('extra_pages/page'.$id.'.html.twig',["cookie_accepted"=>$cookie_disclaimer]);
	}
	
	#[Route('/set_disclaimer_cookie/{var}', name: 'set_disclaimer_cookie')]
	public function set_disclaimer_cookie($var="not_set"): JsonResponse
	{
		$response = new JsonResponse();
		if($var=="set")
		{
			$response->headers->setCookie( Cookie::create('disclaimer_cookie', "true"));
			 $response->setData(["disclaimer_read"=>"true"]);
		}
		else
		{
			$response->headers->setCookie( Cookie::create('disclaimer_cookie', "false"));
			$response->setData(["disclaimer_read"=>"false"]);
		}
		return $response;
	}
	
	#[Route('/get_disclaimer_cookie', name: 'get_disclaimer_cookie')]
	public function get_disclaimer_cookie_http(Request $request): JsonResponse
	{
		$response = new JsonResponse();
		$response->setData(["disclaimer_read"=>$this->get_disclaimer_cookie($request)]);
		return $response;
	}
	

	
	protected function get_disclaimer_cookie($request)
	{
		$disc=$request->cookies->get('disclaimer_cookie',"false");
		return $disc;
		
	}
}