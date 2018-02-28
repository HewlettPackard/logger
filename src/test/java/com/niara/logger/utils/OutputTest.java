/*
* (C) Copyright [2018] Hewlett Packard Enterprise Development LP.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.niara.logger.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class OutputTest {

    @Test
    public void testValueAndTimestamp() {
        String expectedValue = "{\"sensor\":2000000,\"@timestamp\":\"2016-07-06T02:40:26.000Z\",\"source_subtype\":\"AD\",\"source\":\"thing1.niara.com\",\"data\":{\"computer\":\"thing1.niara.com\",\"raw\":\"'THING1 2016-07-05T19:40:26.000-07:00 07/05/2016 07:40:26 PM\\\\nLogName=Security\\\\nSourceName=Microsoft Windows security auditing.\\\\nEventCode=4769\\\\nEventType=0\\\\nType=Information\\\\nComputerName=THING1.niara.com\\\\nTaskCategory=Kerberos Service Ticket Operations\\\\nOpCode=Info\\\\nRecordNumber=414549033\\\\nKeywords=Audit Success\\\\nMessage=A Kerberos service ticket was requested.\\\\n\\\\nAccount Information:\\\\n\\\\tAccount Name:\\\\t\\\\tHELIUM$@NIARA.COM\\\\n\\\\tAccount Domain:\\\\t\\\\tNIARA.COM\\\\n\\\\tLogon GUID:\\\\t\\\\t{319BE684-6F52-2108-CFCA-4BBA528079A0}\\\\n\\\\nService Information:\\\\n\\\\tService Name:\\\\t\\\\tHELIUM$\\\\n\\\\tService ID:\\\\t\\\\tNIARA\\\\\\\\HELIUM$\\\\n\\\\nNetwork Information:\\\\n\\\\tClient Address:\\\\t\\\\t::ffff:10.43.10.13\\\\n\\\\tClient Port:\\\\t\\\\t60159\\\\n\\\\nAdditional Information:\\\\n\\\\tTicket Options:\\\\t\\\\t0x40810000\\\\n\\\\tTicket Encryption Type:\\\\t0x12\\\\n\\\\tFailure Code:\\\\t\\\\t0x0\\\\n\\\\tTransited Services:\\\\t-\\\\n\\\\nThis event is generated every time access is requested to a resource such as a computer or a Windows service.  The service name indicates the resource to which access was requested.\\\\n\\\\nThis event can be correlated with Windows logon events by comparing the Logon GUID fields in each event.  The logon event occurs on the machine that was accessed, which is often a different machine than the domain controller which issued the service ticket.\\\\n\\\\nTicket options, encryption types, and failure codes are defined in RFC 4120.'\",\"keywords\":\"Audit Success\",\"tag\":\"4769\",\"sourcename\":\"Microsoft Windows security auditing.\",\"event_id\":\"4769\",\"type\":\"Information\",\"opcode\":\"Info\",\"timestamp\":\"1467772826000000\",\"log_name\":\"Security\",\"source\":\"thing1.niara.com\",\"subcategory\":\"Kerberos Service Ticket Operations\",\"insertion_string\":{\"account_information.logon_guid\":\"{319BE684-6F52-2108-CFCA-4BBA528079A0}\",\"service_information.service_name\":\"HELIUM$\",\"additional_information.transited_services\":\"-\",\"service_information.service_id\":\"NIARA\\\\HELIUM$\",\"additional_information.failure_code\":\"0x0\",\"network_information.client_address\":\"10.43.10.13\",\"account_information.account_domain\":\"NIARA.COM\",\"additional_information.ticket_encryption_type\":\"0x12\",\"additional_information.ticket_options\":\"0x40810000\",\"network_information.client_port\":\"60159\",\"logon_entity_type\":\"machine\",\"account_information.account_name\":\"HELIUM$\"},\"eventtype\":\"0\",\"log_time\":\"20160705194026.000000-000\",\"record_number\":\"414549033\"},\"source_type\":\"Logs\",\"type\":\"ad_log\",\"collector_type\":\"splunk\"}\n";
        Long expectedTimestamp = 1427182980000L;
        Output output = new Output(expectedValue, expectedTimestamp);
        Object actualValue = output.value();
        Object actualTimestamp = output.timestamp();
        Assert.assertEquals(expectedValue, actualValue);
        Assert.assertEquals(expectedTimestamp, actualTimestamp);
    }
}
