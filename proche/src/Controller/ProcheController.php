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
	
	
	
	protected function escapeSolr($helper, $input)
	{
		return  trim($helper->escapePhrase($input),'"');
	}
	
	protected function escapeSolrArray($helper, $vals)
	{
		$returned=[];
		foreach($vals as $val)
		{
			$returned[]=$helper->escapePhrase( $val);
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
		$lang=$request->cookies->get('proche_locale','en');
		return $lang;
		
	}
	
	#[Route('/', name: 'home')]	
    public function home(Request $request): Response
    {		
		$lang=$this->get_lang_cookie($request);
		$this->localeSwitcher->setLocale($request->getSession()->get('current_locale',$lang));
		$dyna_field_free=$this->getParameter('free_text_search_field',[]);		
		$dyna_field_details=$this->getParameter('detailed_search_fields',[]);
        return $this->render('home.html.twig',["dyna_field_free"=>$dyna_field_free, "dyna_field_details"=>$dyna_field_details]);
    }
	
	#[Route('/{locale}', name: 'homelang', requirements: ['locale' => '(en|fr|nl)'])]	
    public function homelang(Request $request, $locale="en"): Response
    {
		$cookie_locale=$request->cookies->get('proche_locale',"");
		
		$session=$request->getSession();
		$this->localeSwitcher->setLocale($locale);
		$session->set('current_locale', $locale);
		$this->set_lang_cookie($locale);
		$dyna_field_free=$this->getParameter('free_text_search_field',[]);
		$dyna_field_details=$this->getParameter('detailed_search_fields',[]);
        return $this->render('home.html.twig',["dyna_field_free"=>$dyna_field_free, "dyna_field_details"=>$dyna_field_details]);
    }
	
	#[Route('/detailed_searches', name: 'detailed_search')]	
    public function home_detail(Request $request): Response
    {
		
		$dyna_field_details=$this->getParameter('detailed_search_fields',[]);
		$dyna_field_free=$this->getParameter('free_text_search_field',[]);
		
		$this->localeSwitcher->setLocale($request->getSession()->get('current_locale','en'));
		$response = new Response();
		
        return $this->render('home_details.html.twig',["dyna_field_free"=>$dyna_field_free, "dyna_field_details"=>$dyna_field_details]);
    }
	
	
	#[Route('/detail', name: 'detail')]
	public function detail(Request $request): Response
    {
		
		$this->localeSwitcher->setLocale($request->getSession()->get('current_locale','en'));		
		$this->set_lang_cookie( $request->getSession()->get('current_locale','en'));
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
					return $this->render('detail.html.twig',[
						"doc"=>$detail, 
						"detail_main_title_field"=> $detail_main_title_field, 
						"detail_sub_title_field"=> $detail_sub_title_field, 
						"detail_fields"=>$detail_fields]);
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
	
	protected function createFacet($facet_set, $name_facet, $name_field, $limit=10)
	{
		$facet_set->createFacetField($name_facet)->setField($name_field)->setMinCount(1)->setSort('count desc')->setLimit($limit);
	}
	
	protected function returnSolrFacet($facet_set, $name_facet)
	{
		$facets_title =$facet_set->getFacet($name_facet);
		$results=[];
		foreach($facets_title as $vf => $count) 
		{
			if($count>0)
			{
				$results[$vf]=$count;
			}
				
		}
		arsort($results);
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
		$this->localeSwitcher->setLocale($request->getSession()->get('current_locale','en'));
		$this->set_lang_cookie( $request->getSession()->get('current_locale','en'));
		$client=$this->client;
		$query = $client->createSelect();
		$helper = $query->getHelper();
		
		$nb_result=0;
		$rs=[];
		$current_page=$request->get("current_page",1);
        $page_size=$request->get("page_size",$this->page_size);
		$display_facets=$request->get("display_facets",[]);
		
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
		if(array_key_exists("fields",$dyna_field_details ))
		{
			$dyna_fields=$dyna_field_details["fields"];
			foreach($dyna_fields as $field)
			{
				$list_detailed_fields[$field]=$this->escapeSolrArray($helper,$request->get("search_".$field,[]));
			}
		}		
			
		$params_and=[];
		
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
				$params_and[]="(".$field.": (".implode($this->test_and_or($field, $request), $filter)."))";
			}
		}
		
	
		if(count($params_and)>0)
		{
			$query_build=implode(" AND ", $params_and);
		
			$query->setQuery($query_build);
		}
		
		if(count($params_and)>0)
		{
			$query_build=implode(" AND ", $params_and);
		
			$query->setQuery($query_build);
			
			
		}
		if(count($params_and)>0||$go)
		{
			if(count($params_and)>0)
			{
				$q_pattern=implode(" AND ",$params_and); 
			}
			
			
			$query->addSort("sort_number", $query::SORT_ASC);
			
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

				return $this->render('results.html.twig',["results"=>$rs, "nb_result"=>$nb_result, "page_size"=>$page_size, "pagination"=>$pagination,
				'page' => $current_page,
				'dyna_field_facets'=>$facets_twig,
				"display_facets"=>$display_facets,
				"title_field"=>$title_field,
				"link_field"=>$link_field,
				"result_fields"=>$result_fields ]);
			}
		}
	
		
		return $this->render('noresults.html.twig');
	}
}