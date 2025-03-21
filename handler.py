import runpod
import requests
import json
from typing import Dict, List, Any, Optional, Union

def map_equipment_code_to_rateview(equipment_code: str) -> str:
    """Maps load search API equipment codes to Rateview API equipment types."""
    van_codes = ["V", "VA", "VB", "VC", "V2", "VZ", "VH", "VI", "VN", "VG", "VL", "VV", "VM", "VT", "VF", "VR", "VP", "VW"]
    reefer_codes = ["R", "RA", "R2", "RZ", "RN", "RL", "RM", "RG", "RV", "RP"]
    flatbed_codes = ["F", "FA", "FT", "FM", "FD", "FR", "FO", "FN", "FS"]
    
    if equipment_code in van_codes:
        return "VAN"
    elif equipment_code in reefer_codes:
        return "REEFER"
    elif equipment_code in flatbed_codes:
        return "FLATBED"
    else:
        # Default to FLATBED if unknown
        return "FLATBED"

def get_broker_rate_per_mile(load: Dict[str, Any]) -> Optional[float]:
    """Extract broker rate per mile from load data."""
    # Try estimated rate per mile first
    if load.get("estimatedRatePerMile", 0) > 0:
        return load["estimatedRatePerMile"]
    
    # Try to calculate from total rate and trip length
    trip_length_miles = load.get("tripLength", {}).get("miles", 0)
    if trip_length_miles <= 0:
        return None
    
    # Check for privateNetworkRateInfo
    private_rate = load.get("privateNetworkRateInfo", {}).get("bookable", {}).get("rate", {}).get("rateUsd", 0)
    if private_rate > 0:
        return private_rate / trip_length_miles
    
    # Check for loadBoardRateInfo
    load_board_rate = load.get("loadBoardRateInfo", {}).get("nonBookable", {}).get("rateUsd", 0)
    if load_board_rate > 0:
        return load_board_rate / trip_length_miles
    
    return None

def get_total_load_amount(load: Dict[str, Any]) -> Optional[float]:
    """Extract total load amount from load data."""
    # Check for privateNetworkRateInfo
    private_rate = load.get("privateNetworkRateInfo", {}).get("bookable", {}).get("rate", {}).get("rateUsd", 0)
    if private_rate > 0:
        return private_rate
    
    # Check for loadBoardRateInfo
    load_board_rate = load.get("loadBoardRateInfo", {}).get("nonBookable", {}).get("rateUsd", 0)
    if load_board_rate > 0:
        return load_board_rate
    
    return None

def calculate_driver_pay(load: Dict[str, Any]) -> Dict[str, Union[float, str]]:
    """
    Calculate driver pay as 25% of total load amount. 
    If load amount not available, calculate as rate per mile * trip length.
    """
    trip_length_miles = load.get("tripLength", {}).get("miles", 0)
    
    # Try to get total load amount
    total_load_amount = get_total_load_amount(load)
    
    if total_load_amount and total_load_amount > 0:
        # Driver pay is 25% of total load amount
        driver_pay = total_load_amount * 0.25
        source = "percentage_of_total"
    else:
        # Get rate per mile
        rate_per_mile = get_broker_rate_per_mile(load)
        
        if rate_per_mile and rate_per_mile > 0 and trip_length_miles > 0:
            # Calculate total from rate per mile and trip length
            total_calculated = rate_per_mile * trip_length_miles
            driver_pay = total_calculated * 0.25
            source = "calculated_from_rate_per_mile"
        else:
            # Not enough data to calculate
            return {
                "amount": "Not Available",
                "calculation_method": "insufficient_data"
            }
    
    return {
        "amount": round(driver_pay, 2),
        "calculation_method": source
    }

def get_rate_comparison(load_rate: Optional[float], market_rate: Optional[float]) -> Dict[str, Union[float, str]]:
    """Calculate percentage difference between broker rate and market rate."""
    if load_rate is None or load_rate <= 0 or market_rate is None or market_rate <= 0:
        return {
            "broker_rate_per_mile": "Not Available" if load_rate is None or load_rate <= 0 else load_rate,
            "market_rate_per_mile": "Not Available" if market_rate is None or market_rate <= 0 else market_rate,
            "difference_percentage": "N/A",
            "comparison": "Rate comparison not possible"
        }
    
    difference_percentage = ((load_rate - market_rate) / market_rate) * 100
    
    comparison = (
        f"{abs(round(difference_percentage, 2))}% above market rate"
        if difference_percentage > 0
        else f"{abs(round(difference_percentage, 2))}% below market rate"
        if difference_percentage < 0
        else "At market rate"
    )
    
    return {
        "broker_rate_per_mile": load_rate,
        "market_rate_per_mile": market_rate,
        "difference_percentage": round(difference_percentage, 2),
        "comparison": comparison
    }

def call_rateview_api(origin: Dict[str, str], destination: Dict[str, str], 
                     equipment: str, access_token: str) -> Dict[str, Any]:
    """Call the Rateview API to get market rate information."""
    base_url = "https://analytics.api.staging.dat.com/linehaulrates"
    endpoint = "/v1/lookups"
    url = base_url + endpoint
    
    # Format the payload
    payload = [{
        "origin": {
            "city": origin.get("city", ""),
            "stateOrProvince": origin.get("stateProv", "")
        },
        "destination": {
            "city": destination.get("city", ""),
            "stateOrProvince": destination.get("stateProv", "")
        },
        "rateType": "SPOT",
        "equipment": equipment,
        "includeMyRate": True,
        "targetEscalation": {
            "escalationType": "BEST_FIT"
        }
    }]
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code in (200, 201):
            return response.json()
        else:
            return {"error": f"API Error: {response.status_code}", "detail": response.text}
    except Exception as e:
        return {"error": f"Exception: {str(e)}"}

def process_loads_and_compare_rates(loads_data: Dict[str, Any], access_token: str) -> Dict[str, Any]:
    """
    Process loads data, call Rateview API, and calculate rate comparisons.
    
    Args:
        loads_data: The response from the load search API
        access_token: Access token for the Rateview API
        
    Returns:
        Dictionary with processed data and comparisons
    """
    result = {
        "matchCounts": loads_data.get("matchCounts", {}),
        "processedMatches": []
    }
    
    matches = loads_data.get("matches", [])
    
    for match in matches:
        match_id = match.get("matchId", "")
        
        # Extract origin and destination
        matching_asset_info = match.get("matchingAssetInfo", {})
        origin = matching_asset_info.get("origin", {})
        destination_place = matching_asset_info.get("destination", {}).get("place", {})
        
        # Skip if missing critical data
        if not origin or not destination_place:
            continue
        
        # Extract equipment type and map to Rateview format
        equipment_code = matching_asset_info.get("equipmentType", "")
        rateview_equipment = map_equipment_code_to_rateview(equipment_code)
        
        # Get broker rate per mile
        broker_rate_per_mile = get_broker_rate_per_mile(match)
        
        # Calculate driver pay
        driver_pay = calculate_driver_pay(match)
        
        # Call Rateview API
        rateview_response = call_rateview_api(origin, destination_place, rateview_equipment, access_token)
        
        # Extract market rate per mile
        market_rate_per_mile = None
        market_data = None
        rate_data = None
        
        # Process rateview response
        try:
            # Check for API errors
            if "error" in rateview_response:
                market_data = {"error": rateview_response.get("error")}
            else:
                # Get the first rate response
                rate_responses = rateview_response.get("rateResponses", [])
                if rate_responses and len(rate_responses) > 0:
                    response_obj = rate_responses[0].get("response", {})
                    
                    # Extract rate data
                    if "rate" in response_obj:
                        rate_data = response_obj["rate"]
                        
                        # Extract market rate per mile if available
                        if "perMile" in rate_data and "rateUsd" in rate_data["perMile"]:
                            market_rate_per_mile = rate_data["perMile"]["rateUsd"]
                    
                    # Store complete market data
                    market_data = response_obj
        except Exception as e:
            market_data = {"error": f"Error processing rate data: {str(e)}"}
        
        # Calculate comparison
        comparison = get_rate_comparison(broker_rate_per_mile, market_rate_per_mile)
        
        # Build processed match data
        processed_match = {
            "matchId": match_id,
            "origin": {
                "city": origin.get("city", ""),
                "state": origin.get("stateProv", "")
            },
            "destination": {
                "city": destination_place.get("city", ""),
                "state": destination_place.get("stateProv", "")
            },
            "equipmentType": {
                "code": equipment_code,
                "rateviewType": rateview_equipment
            },
            "tripMiles": match.get("tripLength", {}).get("miles", 0),
            "rateComparison": comparison,
            "driver_pay": driver_pay
        }
        
        # Add Rateview market data if available
        if rate_data:
            processed_match["marketData"] = {
                "mileage": rate_data.get("mileage"),
                "reports": rate_data.get("reports"),
                "companies": rate_data.get("companies"),
                "standardDeviation": rate_data.get("standardDeviation"),
                "perMile": rate_data.get("perMile", {}),
                "perTrip": rate_data.get("perTrip", {}),
                "averageFuelSurchargePerMileUsd": rate_data.get("averageFuelSurchargePerMileUsd"),
                "averageFuelSurchargePerTripUsd": rate_data.get("averageFuelSurchargePerTripUsd")
            }
            
            # Add escalation data if available
            if market_data and "escalation" in market_data:
                processed_match["marketData"]["escalation"] = market_data["escalation"]
        elif market_data and "error" in market_data:
            processed_match["marketData"] = {"error": market_data["error"]}
        
        result["processedMatches"].append(processed_match)
    
    return result

def process_freight_data(loads_data: Dict[str, Any], access_token: str) -> Dict[str, Any]:
    """
    Process freight data and return structured results.
    
    Args:
        loads_data: The response from the load search API
        access_token: Access token for the Rateview API
        
    Returns:
        Dictionary with processed data and comparisons
    """
    return process_loads_and_compare_rates(loads_data, access_token)

def handler(job):
    """
    Runpod serverless handler function.
    
    Args:
        job: Contains the job input with freight data and access token
        
    Returns:
        Processed results with rate comparisons
    """
    job_input = job["input"]
    
    # Validate input
    if not isinstance(job_input, dict):
        return {"error": "Input must be a dictionary"}
    
    # Extract required parameters
    freight_data = job_input.get("freight_data")
    access_token = job_input.get("access_token")
    
    # Validate parameters
    if not freight_data:
        return {"error": "Missing required parameter: freight_data"}
    
    if not access_token:
        return {"error": "Missing required parameter: access_token"}
    
    # Process the data and return results
    try:
        result = process_freight_data(freight_data, access_token)
        return result
    except Exception as e:
        return {"error": f"Processing error: {str(e)}"}

# Start the Runpod serverless function
runpod.serverless.start({"handler": handler})
# import runpod
# import requests
# import json
# from typing import Dict, List, Any, Optional, Union

# def map_equipment_code_to_rateview(equipment_code: str) -> str:
#     """Maps load search API equipment codes to Rateview API equipment types."""
#     van_codes = ["V", "VA", "VB", "VC", "V2", "VZ", "VH", "VI", "VN", "VG", "VL", "VV", "VM", "VT", "VF", "VR", "VP", "VW"]
#     reefer_codes = ["R", "RA", "R2", "RZ", "RN", "RL", "RM", "RG", "RV", "RP"]
#     flatbed_codes = ["F", "FA", "FT", "FM", "FD", "FR", "FO", "FN", "FS"]
    
#     if equipment_code in van_codes:
#         return "VAN"
#     elif equipment_code in reefer_codes:
#         return "REEFER"
#     elif equipment_code in flatbed_codes:
#         return "FLATBED"
#     else:
#         # Default to FLATBED if unknown
#         return "FLATBED"

# def get_broker_rate_per_mile(load: Dict[str, Any]) -> Optional[float]:
#     """Extract broker rate per mile from load data."""
#     # Try estimated rate per mile first
#     if load.get("estimatedRatePerMile", 0) > 0:
#         return load["estimatedRatePerMile"]
    
#     # Try to calculate from total rate and trip length
#     trip_length_miles = load.get("tripLength", {}).get("miles", 0)
#     if trip_length_miles <= 0:
#         return None
    
#     # Check for privateNetworkRateInfo
#     private_rate = load.get("privateNetworkRateInfo", {}).get("bookable", {}).get("rate", {}).get("rateUsd", 0)
#     if private_rate > 0:
#         return private_rate / trip_length_miles
    
#     # Check for loadBoardRateInfo
#     load_board_rate = load.get("loadBoardRateInfo", {}).get("nonBookable", {}).get("rateUsd", 0)
#     if load_board_rate > 0:
#         return load_board_rate / trip_length_miles
    
#     return None

# def get_rate_comparison(load_rate: Optional[float], market_rate: Optional[float]) -> Dict[str, Union[float, str]]:
#     """Calculate percentage difference between broker rate and market rate."""
#     if load_rate is None or load_rate <= 0 or market_rate is None or market_rate <= 0:
#         return {
#             "broker_rate_per_mile": "Not Available" if load_rate is None or load_rate <= 0 else load_rate,
#             "market_rate_per_mile": "Not Available" if market_rate is None or market_rate <= 0 else market_rate,
#             "difference_percentage": "N/A",
#             "comparison": "Rate comparison not possible"
#         }
    
#     difference_percentage = ((load_rate - market_rate) / market_rate) * 100
    
#     comparison = (
#         f"{abs(round(difference_percentage, 2))}% above market rate"
#         if difference_percentage > 0
#         else f"{abs(round(difference_percentage, 2))}% below market rate"
#         if difference_percentage < 0
#         else "At market rate"
#     )
    
#     return {
#         "broker_rate_per_mile": load_rate,
#         "market_rate_per_mile": market_rate,
#         "difference_percentage": round(difference_percentage, 2),
#         "comparison": comparison
#     }

# def call_rateview_api(origin: Dict[str, str], destination: Dict[str, str], 
#                      equipment: str, access_token: str) -> Dict[str, Any]:
#     """Call the Rateview API to get market rate information."""
#     base_url = "https://analytics.api.staging.dat.com/linehaulrates"
#     endpoint = "/v1/lookups"
#     url = base_url + endpoint
    
#     # Format the payload
#     payload = [{
#         "origin": {
#             "city": origin.get("city", ""),
#             "stateOrProvince": origin.get("stateProv", "")
#         },
#         "destination": {
#             "city": destination.get("city", ""),
#             "stateOrProvince": destination.get("stateProv", "")
#         },
#         "rateType": "SPOT",
#         "equipment": equipment,
#         "includeMyRate": True,
#         "targetEscalation": {
#             "escalationType": "BEST_FIT"
#         }
#     }]
    
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {access_token}"
#     }
    
#     try:
#         response = requests.post(url, headers=headers, data=json.dumps(payload))
#         if response.status_code in (200, 201):
#             return response.json()
#         else:
#             return {"error": f"API Error: {response.status_code}", "detail": response.text}
#     except Exception as e:
#         return {"error": f"Exception: {str(e)}"}

# def process_loads_and_compare_rates(loads_data: Dict[str, Any], access_token: str) -> Dict[str, Any]:
#     """
#     Process loads data, call Rateview API, and calculate rate comparisons.
    
#     Args:
#         loads_data: The response from the load search API
#         access_token: Access token for the Rateview API
        
#     Returns:
#         Dictionary with processed data and comparisons
#     """
#     result = {
#         "matchCounts": loads_data.get("matchCounts", {}),
#         "processedMatches": []
#     }
    
#     matches = loads_data.get("matches", [])
    
#     for match in matches:
#         match_id = match.get("matchId", "")
        
#         # Extract origin and destination
#         matching_asset_info = match.get("matchingAssetInfo", {})
#         origin = matching_asset_info.get("origin", {})
#         destination_place = matching_asset_info.get("destination", {}).get("place", {})
        
#         # Skip if missing critical data
#         if not origin or not destination_place:
#             continue
        
#         # Extract equipment type and map to Rateview format
#         equipment_code = matching_asset_info.get("equipmentType", "")
#         rateview_equipment = map_equipment_code_to_rateview(equipment_code)
        
#         # Get broker rate per mile
#         broker_rate_per_mile = get_broker_rate_per_mile(match)
        
#         # Call Rateview API
#         rateview_response = call_rateview_api(origin, destination_place, rateview_equipment, access_token)
        
#         # Extract market rate per mile
#         market_rate_per_mile = None
#         market_data = None
#         rate_data = None
        
#         # Process rateview response
#         try:
#             # Check for API errors
#             if "error" in rateview_response:
#                 market_data = {"error": rateview_response.get("error")}
#             else:
#                 # Get the first rate response
#                 rate_responses = rateview_response.get("rateResponses", [])
#                 if rate_responses and len(rate_responses) > 0:
#                     response_obj = rate_responses[0].get("response", {})
                    
#                     # Extract rate data
#                     if "rate" in response_obj:
#                         rate_data = response_obj["rate"]
                        
#                         # Extract market rate per mile if available
#                         if "perMile" in rate_data and "rateUsd" in rate_data["perMile"]:
#                             market_rate_per_mile = rate_data["perMile"]["rateUsd"]
                    
#                     # Store complete market data
#                     market_data = response_obj
#         except Exception as e:
#             market_data = {"error": f"Error processing rate data: {str(e)}"}
        
#         # Calculate comparison
#         comparison = get_rate_comparison(broker_rate_per_mile, market_rate_per_mile)
        
#         # Build processed match data
#         processed_match = {
#             "matchId": match_id,
#             "origin": {
#                 "city": origin.get("city", ""),
#                 "state": origin.get("stateProv", "")
#             },
#             "destination": {
#                 "city": destination_place.get("city", ""),
#                 "state": destination_place.get("stateProv", "")
#             },
#             "equipmentType": {
#                 "code": equipment_code,
#                 "rateviewType": rateview_equipment
#             },
#             "tripMiles": match.get("tripLength", {}).get("miles", 0),
#             "rateComparison": comparison
#         }
        
#         # Add Rateview market data if available
#         if rate_data:
#             processed_match["marketData"] = {
#                 "mileage": rate_data.get("mileage"),
#                 "reports": rate_data.get("reports"),
#                 "companies": rate_data.get("companies"),
#                 "standardDeviation": rate_data.get("standardDeviation"),
#                 "perMile": rate_data.get("perMile", {}),
#                 "perTrip": rate_data.get("perTrip", {}),
#                 "averageFuelSurchargePerMileUsd": rate_data.get("averageFuelSurchargePerMileUsd"),
#                 "averageFuelSurchargePerTripUsd": rate_data.get("averageFuelSurchargePerTripUsd")
#             }
            
#             # Add escalation data if available
#             if market_data and "escalation" in market_data:
#                 processed_match["marketData"]["escalation"] = market_data["escalation"]
#         elif market_data and "error" in market_data:
#             processed_match["marketData"] = {"error": market_data["error"]}
        
#         result["processedMatches"].append(processed_match)
    
#     return result

# def process_freight_data(loads_data: Dict[str, Any], access_token: str) -> Dict[str, Any]:
#     """
#     Process freight data and return structured results.
    
#     Args:
#         loads_data: The response from the load search API
#         access_token: Access token for the Rateview API
        
#     Returns:
#         Dictionary with processed data and comparisons
#     """
#     return process_loads_and_compare_rates(loads_data, access_token)

# def handler(job):
#     """
#     Runpod serverless handler function.
    
#     Args:
#         job: Contains the job input with freight data and access token
        
#     Returns:
#         Processed results with rate comparisons
#     """
#     job_input = job["input"]
    
#     # Validate input
#     if not isinstance(job_input, dict):
#         return {"error": "Input must be a dictionary"}
    
#     # Extract required parameters
#     freight_data = job_input.get("freight_data")
#     access_token = job_input.get("access_token")
    
#     # Validate parameters
#     if not freight_data:
#         return {"error": "Missing required parameter: freight_data"}
    
#     if not access_token:
#         return {"error": "Missing required parameter: access_token"}
    
#     # Process the data and return results
#     try:
#         result = process_freight_data(freight_data, access_token)
#         return result
#     except Exception as e:
#         return {"error": f"Processing error: {str(e)}"}

# # Start the Runpod serverless function
# runpod.serverless.start({"handler": handler})
