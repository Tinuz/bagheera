/*
 * Copyright 2011 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.bagheera.rest.interceptors;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;


/**
 * Utility class for aggregating incoming pings with existing documents.
 */
public abstract class AbstractPreCommitHook implements PreCommitHook {

	@Override
	public void setRequest(HttpServletRequest request) {
		// Do nothing
	}

	@Override
	public boolean isCustomResponseRequired() {
		return false;
	}

	@Override
	public Response getCustomResponse() {
		return null;
	}
	
}