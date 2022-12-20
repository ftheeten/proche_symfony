<?php
// src/Controller/ProcheController.php
namespace App\Controller;

use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\Routing\Annotation\Route;
use Solarium\Client;
//use Solarium\Core\Client\Adapter\Curl;
//use App\Service\SiteUpdateManager;
//use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\HttpFoundation\JsonResponse;

class ProcheController extends AbstractController
{

    private $client;
	private $page_size=10;
	
	
	
	/** @var \Solarium\Client */
   public function __construct(\Solarium\Client $client) {
       $this->client = $client;
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
	
	#[Route('/', name: 'home')]	
    public function home(): Response
    {
		//$this->init_client();
        return $this->render('home.html.twig',[]);
    }
	
	#[Route('/detail', name: 'detail')]
	public function detail(Request $request): Response
    {
		$client=$this->client;
		$id=$request->get("q","");
		if(is_numeric($id))
		{
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
					return $this->render('detail.html.twig',["doc"=>$detail]);
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
	
	
	#[Route('/main_search', name: 'main_search')]
	public function main_search(Request $request): Response
    {
		$client=$this->client;
		$query = $client->createSelect();
		$helper = $query->getHelper();
		
		$nb_result=0;
		$rs=[];
		$current_page=$request->get("current_page",1);
        $page_size=$request->get("page_size",$this->page_size);
		$offset=(((int)$current_page)-1)* (int)$page_size;
		$pagination=Array();
		
		$free_search=$this->escapeSolr($helper,$request->get("free_search",""));
		print($free_search);
		$search_colnum=$this->escapeSolrArray($helper,$request->get("search_colnum",[]));
		$search_desc=$this->escapeSolrArray($helper,$request->get("search_desc",[]));
		$search_culture=$this->escapeSolrArray($helper,$request->get("search_culture",[]));
		$search_geo=$this->escapeSolrArray($helper, $request->get("search_geo",[]));
		$search_const=$this->escapeSolrArray($helper,$request->get("search_const",[]));
		$search_acq=$this->escapeSolrArray($helper,$request->get("search_acq",[]));
		$search_medium=$this->escapeSolrArray($helper,$request->get("search_medium",[]));
		
		$params_and=[];
		
		
		$query->setStart($offset)->setRows($page_size);
		$go=false;
		if(strlen(trim($free_search))>0)
		{
			$go=true;
			print("all:*".$free_search."*");
			$query->setQuery("all:*".$free_search."*");
		}
		if(count($search_colnum)>0)
		{
			
		
			$params_and[]="(object_number: (".implode(" OR ", $search_colnum)."))";
			
		}
		
		if(count($search_desc)>0)
		{
			$params_and[]="(FACET_descriptions: (".implode(" OR ", $search_desc)."))";			
		}
		
		if(count($search_culture)>0)
		{
			$params_and[]="(FACET_cultures: (".implode(" OR ", $search_culture)."))";			
		}
		
		if(count($search_geo)>0)
		{
			$params_and[]="(FACET_geographies: (".implode(" OR ", $search_geo)."))";			
		}
		
		if(count($search_const)>0)
		{
			$params_and[]="(FACET_constituents: (".implode(" OR ", $search_const)."))";			
		}
		
		if(count($search_acq)>0)
		{
			$params_and[]="(acquisition_method: (".implode(" OR ", $search_acq)."))";			
		}
		
		if(count($search_medium)>0)
		{
			$params_and[]="(medium: (".implode(" OR ", $search_medium)."))";			
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
			$q_pattern=implode(" AND ",$params_and); 
			
			
			
			$query->addSort("sort_number", $query::SORT_ASC);
			$rs_tmp= $client->select($query);
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

			return $this->render('results.html.twig',["results"=>$rs, "nb_result"=>$nb_result, "page_size"=>$page_size, "pagination"=>$pagination]);
		}
		else
		{
			$query->setQuery("all:*".$free_search."*");
			$rs_tmp= $client->select($query);
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

			return $this->render('results.html.twig',["results"=>$rs, "nb_result"=>$nb_result, "page_size"=>$page_size, "pagination"=>$pagination]);
		}
		
		return $this->render('noresults.html.twig');
	}
}