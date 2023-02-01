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

class ProcheController extends AbstractController
{


	protected $default_lang="fr";
    private $client;
	private $page_size=10;

	
	private $localeSwitcher;
	//private $session;

	private $list_included_fields_csv=["id","object_number", "sort_number",	"title"	, "dimensions",	"date_of_acquisition","acquisition_method",	"creation_date","culture","objtitle_legacy","_version_",	"score"];

	
	
	
	/** @var \Solarium\Client */
   public function __construct(\Solarium\Client $client,  LocaleSwitcher $localeSwitcher ) {
	   
       $this->client = $client;
	   $this->localeSwitcher=$localeSwitcher;
	  
    }
	
	
	protected function remove_punctuation($val)
	{
		$val=str_replace("-"," ",$val);
		$val=str_replace(":"," ",$val);
		$val=str_replace(","," ",$val);
		$val=str_replace(";"," ",$val);
		$val=str_replace("!"," ",$val);
		$val=str_replace("?"," ",$val);
		$val=str_replace("."," ",$val);
		$val=str_replace("("," ",$val);
		$val=str_replace(")"," ",$val);
		$val=str_replace("'"," ",$val);
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
			return "*".$tmp."*";
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
			$val=$this->remove_punctuation($val);
			if($fuzzy)
			{
				
				$returned[]="*".trim($helper->escapePhrase( $val),'"')."*";
				
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
		if(is_numeric($id))
		{
			$detail_main_title_field=$this->getParameter("detail_main_title_field", "id");
			$detail_sub_title_field=$this->getParameter("detail_sub_title_field", "id");
			$detail_fields=$this->getParameter("detail_fields",[]);
			if(strlen(trim($id))>0)
			{
				$query = $client->createSelect();
				$query->setQuery("id:".$id);
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
		}
		
		return $this->render('noresults.html.twig');
	}
	
	
	#[Route('/terms', name: 'terms')]
	public function terms(Request $request): JsonResponse
    {
		$returned=[];
		$client=$this->client;
		$str="";
		   $field=$request->get("f","");
		   $value=$request->get("q","");
		  
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
						array_unshift($resp2, ["id"=>$value, "text"=>$value]);
						$returned=[
									"results"=>$resp2
								];
					
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
			  array_unshift($resp, ["id"=>$value, "text"=>$value]);
			$returned=[
						"results"=>$resp2
					];
			 
		   }
		 return $this->json($returned); 	
	}
	
	protected function createFacet($facet_set, $name_facet, $name_field, $limit=100000)
	{
		$facet_set->createFacetField($name_facet)->setField($name_field)->setMinCount(1)->setSort('desc')->setLimit($limit);
	}
	
	protected function returnSolrFacet($facet_set, $name_facet, $limit=10)
	{
		$facets_title =$facet_set->getFacet($name_facet);
		$results=[];
		$i=0;
		//print_r($facets_title);
		foreach($facets_title as $vf => $count) 
		{
			
			if($count>0)
			{
				$results[$vf]=$count;
			}
			
			$i++;	
		}
		arsort($results);
		$results=array_slice($results, 0, $limit, true);
		return $results;
	}
	
	public function test_and_or($field, $request)
	{
		if(strtolower($request->get("chkand_search_".$field,"false"))=="true")
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
		$rs=[];
		$current_page=$request->get("current_page",1);
        $page_size=$request->get("page_size",$this->page_size);
		$display_facets=$request->get("display_facets",[]);
		
		$sort_field=$this->getParameter('sort_field',"id");
		$dyna_field_free=$this->getParameter('free_text_search_field',[]);
		$dyna_field_details=$this->getParameter('detailed_search_fields',[]);
		$dyna_field_facets=$this->getParameter('facet_fields',[]);
		$tech_fields_facets=$dyna_field_facets["fields"];
		$label_facets=$dyna_field_facets["labels"];
		$call_back_facets=$dyna_field_facets["filter_callback"];
		$i=0;
		$facet_labels=[];
		$facet_callbacks=[];
		foreach($tech_fields_facets as $tech_field)
		{
			$facet_labels[$tech_field]=$label_facets[$i];
			$facet_callbacks[$tech_field]="search_".$call_back_facets[$i];
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
		
		//$free_search=$this->escapeSolr($helper,$request->get("free_search",""));
		if(array_key_exists("field",$dyna_field_free)&&array_key_exists("label", $dyna_field_free))
		{
			$free_search=$this->escapeSolrArray($helper,$request->get("free_search",[]));
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
			
		
		
		$query_build="*:*";
		if($csv)
		{
			$query->setStart(0)->setRows(1000000);
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
				$params_and[]="(".$field.": (".implode($this->test_and_or($field, $request),$filter)."))";
			}
		}
		
	
		
		if(count($params_and)>0)
		{
			$query_build=implode(" AND ", $params_and);
			$query->setQuery($query_build);
			
			
		}
		else
		{
			
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
			
			if($csv)
			{
				
				$response = new StreamedResponse();
				$response->setCallback(
					function () use ($rs_tmp) 
					{
						$filler=array_fill_keys($this->list_included_fields_csv, '');
						ob_start();
						$handle = fopen('php://output', 'r+');
						$i=0;
						foreach ($rs_tmp as $document) 
						{
							
							$doc=[];
							foreach ($document as $field => $value) 
							{
								if( in_array($field, $this->list_included_fields_csv))
								{
									if(is_array($value))
									{
										$value=implode(",", $value);
									}
									$doc[$field]=$value;
									
								}
							}
							foreach($this->list_included_fields_csv as $field)
							{
								if(!array_key_exists($field,$doc ))
								{
									$doc[$field]="";
								}
							}
							$order=array_keys($this->list_included_fields_csv);
							uksort($doc, function($key1, $key2) use ($order) {
								return (array_search($key1,$this->list_included_fields_csv) > array_search($key2,$this->list_included_fields_csv));
							});
							if($i==0)
							{
								fputcsv($handle,array_values(array_keys($doc)), "\t");
							}
							fputcsv($handle,array_values($doc), "\t");
							$i++;
						}
							
						
						fclose($handle);
						ob_flush();
					}
				);
				$response->headers->set('Content-Type', 'text/csv');       
				$response->headers->set('Content-Disposition', 'attachment; filename="testing.csv"');
				
				return $response;
			}
			else
			{
								
				$facets_twig=[];
				
				if(count($dyna_field_facets)>0)
				{
					$facet_fields=$dyna_field_facets["fields"];
					$i=0;
					foreach($facet_fields as $facet)
					{
						$facets_twig[$i]["facet"]=$this->returnSolrFacet($rs_tmp->getFacetSet(), "facet_".$facet);
						$facets_twig[$i]["label"]=$facet_labels[$facet];
						$facets_twig[$i]["callback"]=$facet_callbacks[$facet];
						$i++;
					}
				}
				$nb_result=$rs_tmp->getNumFound();
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