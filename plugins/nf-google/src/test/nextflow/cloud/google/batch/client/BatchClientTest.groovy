/*
 * Copyright 2013-2026, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nextflow.cloud.google.batch.client

import java.util.concurrent.TimeoutException

import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.DeadlineExceededException
import com.google.api.gax.rpc.NotFoundException
import com.google.api.gax.rpc.PermissionDeniedException
import com.google.api.gax.rpc.UnauthenticatedException
import com.google.api.gax.rpc.UnavailableException
import com.google.cloud.batch.v1.Task
import com.google.cloud.batch.v1.TaskName
import com.google.cloud.batch.v1.TaskStatus
import io.grpc.Status
import io.grpc.StatusRuntimeException
import nextflow.cloud.google.GoogleOpts
import spock.lang.Specification
import spock.lang.Unroll

/**
 *
 * @author Jorge Ejarque <jorge.ejarque@seqera.io>
 */
class BatchClientTest extends Specification{

    def 'should return task status with getTaskInArray' () {
        given:
        def project = 'project-id'
        def location = 'location-id'
        def job1 = 'job1-id'
        def task1 = 'task1-id'
        def task1Name = TaskName.of(project, location, job1, 'group0', task1).toString()
        def job2 = 'job2-id'
        def task2 = 'task2-id'
        def task2Name = TaskName.of(project, location, job2, 'group0', task2).toString()
        def job3 = 'job3-id'
        def task3 = 'task3-id'
        def task3Name = TaskName.of(project, location, job3, 'group0', task3).toString()
        def arrayTasks = new HashMap<String,TaskStatusRecord>()
        def client = Spy( new BatchClient( projectId: project, location: location, arrayTaskStatus: arrayTasks ) )

        when:
        client.listTasks(job2) >> {
            def list = new LinkedList<>()
            list.add(makeTask(task2Name, TaskStatus.State.FAILED))
            return list
        }
        client.listTasks(job3) >> {
            def list = new LinkedList<>()
            list.add(makeTask(task3Name, TaskStatus.State.SUCCEEDED))
            return list
        }
        arrayTasks.put(task1Name, makeTaskStatusRecord(TaskStatus.State.RUNNING, System.currentTimeMillis()))
        arrayTasks.put(task2Name, makeTaskStatusRecord(TaskStatus.State.PENDING, System.currentTimeMillis() - 1_001))

        then:
        // recent cached task
        client.getTaskInArrayStatus(job1, task1).state == TaskStatus.State.RUNNING
        // Outdated cached task
        client.getTaskInArrayStatus(job2, task2).state == TaskStatus.State.FAILED
        // no cached task
        client.getTaskInArrayStatus(job3, task3).state == TaskStatus.State.SUCCEEDED
    }

    TaskStatusRecord makeTaskStatusRecord(TaskStatus.State state, long timestamp) {
        return new TaskStatusRecord(TaskStatus.newBuilder().setState(state).build(), timestamp)
    }

    def makeTask(String name, TaskStatus.State state){
        Task.newBuilder().setName(name)
            .setStatus(TaskStatus.newBuilder().setState(state).build())
            .build()
    }

    @Unroll
    def 'should determine retry condition for #ERROR' () {
        given:
        def client = new BatchClient()

        expect:
        client.retryCondition(ERROR) == EXPECTED

        where:
        ERROR                                           | EXPECTED
        new IOException('io error')                     | true
        new TimeoutException('timeout')                 | true
        unavailable()                                   | true
        deadlineExceeded()                              | true
        notFound()                                      | true
        new RuntimeException('nope')                    | false
        permissionDenied()                              | false
        new RuntimeException('nope', new RuntimeException('nope')) | false
        new RuntimeException('wrap', new IOException()) | true
    }

    def 'should retry an IO error nested in the cause chain' () {
        given:
        def client = new BatchClient()
        and: 'the error reported when the metadata server fails to refresh the credentials'
        def root = new IOException('Unexpected Error code 500 trying to get security access token from Compute Engine metadata for the default service account')
        def grpc = Status.UNAUTHENTICATED.withDescription('Failed computing credential metadata').withCause(root).asRuntimeException()
        def err = new UnauthenticatedException(grpc, GrpcStatusCode.of(Status.Code.UNAUTHENTICATED), false)

        expect: 'the IO error is two levels deep and must still be detected'
        !IOException.isInstance(err.cause)
        and:
        client.retryCondition(err)
    }

    def 'should retry the action when the credentials refresh fails transiently' () {
        given:
        def client = new BatchClient(config: new GoogleOpts([batch: [retryPolicy: [maxAttempts: 3, delay: '1ms', maxDelay: '10ms']]]))
        and:
        def attempts = 0

        when:
        def result = client.apply(() -> {
            attempts++
            if( attempts < 3 )
                throw unauthenticatedCausedByIO()
            return 'done'
        })

        then:
        result == 'done'
        attempts == 3
    }

    def 'should give up after the max attempts are exhausted' () {
        given:
        def client = new BatchClient(config: new GoogleOpts([batch: [retryPolicy: [maxAttempts: 2, delay: '1ms', maxDelay: '10ms']]]))
        and:
        def attempts = 0

        when:
        client.apply(() -> { attempts++; throw unauthenticatedCausedByIO() })

        then:
        thrown(UnauthenticatedException)
        attempts == 2
    }

    def 'should not retry a non-transient error' () {
        given:
        def client = new BatchClient(config: new GoogleOpts([batch: [retryPolicy: [maxAttempts: 5, delay: '1ms', maxDelay: '10ms']]]))
        and:
        def attempts = 0

        when:
        client.apply(() -> { attempts++; throw permissionDenied() })

        then:
        thrown(PermissionDeniedException)
        attempts == 1
    }

    static private UnauthenticatedException unauthenticatedCausedByIO() {
        final grpc = Status.UNAUTHENTICATED
                .withDescription('Failed computing credential metadata')
                .withCause(new IOException('Unexpected Error code 500'))
                .asRuntimeException()
        return new UnauthenticatedException(grpc, GrpcStatusCode.of(Status.Code.UNAUTHENTICATED), false)
    }

    static private UnavailableException unavailable() {
        new UnavailableException(new StatusRuntimeException(Status.UNAVAILABLE), GrpcStatusCode.of(Status.Code.UNAVAILABLE), true)
    }

    static private DeadlineExceededException deadlineExceeded() {
        new DeadlineExceededException(new StatusRuntimeException(Status.DEADLINE_EXCEEDED), GrpcStatusCode.of(Status.Code.DEADLINE_EXCEEDED), true)
    }

    static private NotFoundException notFound() {
        new NotFoundException(new StatusRuntimeException(Status.NOT_FOUND), GrpcStatusCode.of(Status.Code.NOT_FOUND), false)
    }

    static private PermissionDeniedException permissionDenied() {
        new PermissionDeniedException(new StatusRuntimeException(Status.PERMISSION_DENIED), GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false)
    }

}
